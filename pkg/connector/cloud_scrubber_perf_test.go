package connector

// Tests for the privacy-scrubber performance fix.
//
// Background: scrubBridgedBodies/scrubReactionText run every 5 minutes and
// were full-scanning cloud_message (no index covers body_scrubbed/updated_ts)
// and re-materializing bridgev2's entire message-table id list on every
// 1000-row chunk. On a production DB (276k cloud_message / 261k message rows)
// that made chunks take 40-145s, held SQLite's single write lock, starved the
// 4-connection pool, and delayed live message bridging by up to 10 minutes.
//
// The fix: (1) a partial index cloud_message_scrub_idx on the un-scrubbed
// population, created in ensureSchema; (2) loadBridgedGUIDSet, which reads the
// delivered-guid set once per scrub pass instead of once per chunk.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// createBridgeMessageTable creates the slice of bridgev2's `message` table
// that scrubBridgedBodies joins against. Only the columns the queries touch
// are needed (same fixture shape as TestInstrDialectHelperQueriesRun).
func createBridgeMessageTable(t *testing.T, db *dbutil.Database, ctx context.Context) {
	t.Helper()
	if _, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS message (
		id TEXT NOT NULL,
		bridge_id TEXT NOT NULL,
		room_receiver TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		t.Fatalf("create message table: %v", err)
	}
}

func insertBridgeMessage(t *testing.T, db *dbutil.Database, ctx context.Context, id, bridgeID, receiver string) {
	t.Helper()
	if _, err := db.Exec(ctx,
		`INSERT INTO message (id, bridge_id, room_receiver) VALUES ($1, $2, $3)`,
		id, bridgeID, receiver,
	); err != nil {
		t.Fatalf("insert bridgev2 message row %q: %v", id, err)
	}
}

// TestEnsureSchemaCreatesScrubIndex verifies the partial index exists after
// migration (including on a fresh DB) and that the planner actually picks it
// for the scrubber's candidate scan.
func TestEnsureSchemaCreatesScrubIndex(t *testing.T) {
	ctx := context.Background()
	db := newTestSQLiteDB(t)
	store := newCloudBackfillStore(db, testSQLLoginID)
	if err := store.ensureSchema(ctx); err != nil {
		t.Fatalf("ensureSchema: %v", err)
	}

	var n int
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='cloud_message_scrub_idx'`,
	).Scan(&n); err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if n != 1 {
		t.Fatalf("cloud_message_scrub_idx not created by ensureSchema")
	}

	var indexSQL string
	if err := db.QueryRow(ctx,
		`SELECT COALESCE(sql,'') FROM sqlite_master WHERE name='cloud_message_scrub_idx'`,
	).Scan(&indexSQL); err != nil {
		t.Fatalf("read index sql: %v", err)
	}
	if !strings.Contains(indexSQL, "WHERE body_scrubbed") {
		t.Errorf("index is not partial (missing WHERE body_scrubbed): %s", indexSQL)
	}

	// Planner check: the scrubber's inner candidate query must search via the
	// new index rather than scanning the whole table.
	if err := store.upsertMessageBatch(ctx, []cloudMessageRow{{
		GUID: "plan-check-guid", PortalID: "gid:p", TimestampMS: 1,
		Service: "iMessage",
	}}); err != nil {
		t.Fatalf("upsertMessageBatch: %v", err)
	}
	if _, err := db.Exec(ctx, `ANALYZE`); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}
	rows, err := db.Query(ctx,
		`EXPLAIN QUERY PLAN SELECT guid FROM cloud_message
		 WHERE login_id=$1 AND body_scrubbed=FALSE AND updated_ts < $2`,
		testSQLLoginID, time.Now().UnixMilli(),
	)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var a, b, c int
		var detail string
		if err := rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		plan.WriteString(detail)
		plan.WriteString("\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate plan rows: %v", err)
	}
	got := plan.String()
	if !strings.Contains(got, "cloud_message_scrub_idx") {
		t.Errorf("planner does not use cloud_message_scrub_idx; plan:\n%s", got)
	}
	if strings.Contains(got, "SCAN") && !strings.Contains(got, "COVERING") &&
		!strings.Contains(got, "SEARCH") {
		t.Errorf("candidate query falls back to a full table scan; plan:\n%s", got)
	}
}

// TestLoadBridgedGUIDSet covers the once-per-pass delivered-guid reader:
// case-normalisation, part-suffix stripping, login/receiver scoping, and
// exclusion of other logins' rows.
func TestLoadBridgedGUIDSet(t *testing.T) {
	ctx := context.Background()
	db := newTestSQLiteDB(t)
	store := newCloudBackfillStore(db, testSQLLoginID)
	createBridgeMessageTable(t, db, ctx)

	otherLogin := networkid.UserLoginID("other-login")

	// Delivered for our login: base id form...
	insertBridgeMessage(t, db, ctx, "ABC-1", "b1", string(testSQLLoginID))
	// ...part-suffixed form (normalised back to base)...
	insertBridgeMessage(t, db, ctx, "guid-2_att0", "b1", "")
	// ...uppercase stored id vs mixed-case cloud guid (UPPER matching)...
	insertBridgeMessage(t, db, ctx, strings.ToUpper("guid-3"), "b1", string(testSQLLoginID))
	// Other bridge id → excluded.
	insertBridgeMessage(t, db, ctx, "guid-4", "other-bridge", string(testSQLLoginID))
	// Same bridge but a different room_receiver (another login's portal row)
	// → excluded: only '' or our own receiver count.
	insertBridgeMessage(t, db, ctx, "guid-5", "b1", string(otherLogin))
	// Other login's delivered message entirely → excluded.
	insertBridgeMessage(t, db, ctx, "guid-6", "b1", string(otherLogin))

	set, err := store.loadBridgedGUIDSet(ctx, "b1")
	if err != nil {
		t.Fatalf("loadBridgedGUIDSet: %v", err)
	}

	for guid, want := range map[string]bool{
		"abc-1":  true, // lower-cased from ABC-1
		"guid-2": true, // part suffix stripped
		"guid-3": true,
		"guid-4": false,
		"guid-5": false,
		"guid-6": false,
	} {
		if _, ok := set[guid]; ok != want {
			t.Errorf("set contains %q = %v, want presence=%v", guid, ok, want)
		}
	}
}

// TestScrubBridgedBodiesMultiChunkPreservesBehavior runs a full scrub pass
// over a population big enough to force several chunks and asserts every
// pre-existing semantic is preserved: only delivered+grace-windowed rows are
// cleared, live undelivered rows keep plaintext, soft-deleted rows clear
// without any message-table row, tapbacks stay out of scope, portal
// exclusions hold, and chunking terminates.
func TestScrubBridgedBodiesMultiChunkPreservesBehavior(t *testing.T) {
	ctx := context.Background()
	db := newTestSQLiteDB(t)
	store := newCloudBackfillStore(db, testSQLLoginID)
	if err := store.ensureSchema(ctx); err != nil {
		t.Fatalf("ensureSchema: %v", err)
	}
	createBridgeMessageTable(t, db, ctx)

	const bridgeID = "test-bridge"
	now := time.Now().UnixMilli()
	old := now - int64(time.Hour/time.Millisecond)

	// 2500 old delivered messages → forces ≥3 chunks at chunkSize=1000.
	delivered := make([]cloudMessageRow, 0, 2500)
	for i := 0; i < 2500; i++ {
		guid := fmt.Sprintf("delivered-%04d", i)
		delivered = append(delivered, cloudMessageRow{
			GUID: guid, PortalID: "gid:bulk", TimestampMS: old,
			Text: "secret body " + guid, Sender: "tel:+1555",
			Service: "iMessage", HasBody: true,
		})
		insertBridgeMessage(t, db, ctx, guid, bridgeID, string(testSQLLoginID))
	}
	if err := store.upsertMessageBatch(ctx, delivered); err != nil {
		t.Fatalf("upsert delivered batch: %v", err)
	}

	rows := []cloudMessageRow{
		// Delivered but fresh → grace window protects it this pass.
		{GUID: "fresh-delivered", PortalID: "gid:bulk", TimestampMS: now,
			Text: "fresh secret", Service: "iMessage", HasBody: true},
		// Old but NOT delivered (backfill failed upstream) → must keep text.
		{GUID: "undelivered-old", PortalID: "gid:bulk", TimestampMS: old,
			Text: "undelivered secret", Service: "iMessage", HasBody: true},
		// Old delivered tapback → scrubBridgedBodies must not touch it
		// (scrubReactionText owns reaction rows).
		{GUID: "tapback-old", PortalID: "gid:bulk", TimestampMS: old,
			Text: "Loved 'x'", Service: "iMessage"},
		// Soft-deleted old row with NO message-table row → still cleared.
		{GUID: "deleted-old", PortalID: "gid:bulk", TimestampMS: old,
			Text: "deleted secret", Deleted: true, Service: "iMessage", HasBody: true},
		// Lives in an excluded portal (active restore pipeline) → kept.
		{GUID: "restore-portal-row", PortalID: "gid:restore", TimestampMS: old,
			Text: "restoring secret", Service: "iMessage", HasBody: true},
	}
	tap := uint32(2001)
	rows[2].TapbackType = &tap
	insertBridgeMessage(t, db, ctx, "fresh-delivered", bridgeID, string(testSQLLoginID))
	insertBridgeMessage(t, db, ctx, "restore-portal-row", bridgeID, string(testSQLLoginID))
	if err := store.upsertMessageBatch(ctx, rows); err != nil {
		t.Fatalf("upsert special rows: %v", err)
	}

	// Age every row except the fresh one past the grace window: upsert
	// stamps updated_ts at insert time, so the window never trips otherwise.
	if _, err := db.Exec(ctx,
		`UPDATE cloud_message SET updated_ts=$1 WHERE login_id=$2 AND guid <> $3`,
		old, testSQLLoginID, "fresh-delivered",
	); err != nil {
		t.Fatalf("age messages: %v", err)
	}

	textOf := func(t *testing.T, guid string) sql.NullString {
		t.Helper()
		var s sql.NullString
		if err := db.QueryRow(ctx,
			`SELECT text FROM cloud_message WHERE login_id=$1 AND guid=$2`,
			testSQLLoginID, guid,
		).Scan(&s); err != nil {
			t.Fatalf("read text of %s: %v", guid, err)
		}
		return s
	}

	total, err := store.scrubBridgedBodies(ctx, bridgeID, time.Minute, []string{"gid:restore"})
	if err != nil {
		t.Fatalf("scrubBridgedBodies: %v", err)
	}
	if total != 2501 { // 2500 bulk + deleted-old; nothing else
		t.Errorf("scrubbed %d rows, want 2501", total)
	}
	for _, guid := range []string{"undelivered-old", "fresh-delivered", "restore-portal-row"} {
		if got := textOf(t, guid); !got.Valid || got.String == "" {
			t.Errorf("%s: text was cleared, want preserved", guid)
		}
	}
	if got := textOf(t, "deleted-old"); got.Valid && got.String != "" {
		t.Errorf("deleted-old: text = %q, want NULL (soft-deleted rows clear unconditionally)", got.String)
	}
	var flag bool
	if err := db.QueryRow(ctx,
		`SELECT body_scrubbed FROM cloud_message WHERE login_id=$1 AND guid=$2`,
		testSQLLoginID, "delivered-0000",
	).Scan(&flag); err != nil {
		t.Fatalf("read body_scrubbed: %v", err)
	}
	if !flag {
		t.Errorf("delivered-0000: body_scrubbed = false, want true")
	}
	// Idempotence + drain: a second full pass finds nothing new except rows
	// that aged past the window, and never loops forever.
	again, err := store.scrubBridgedBodies(ctx, bridgeID, time.Minute, []string{"gid:restore"})
	if err != nil {
		t.Fatalf("second scrubBridgedBodies: %v", err)
	}
	if again != 0 {
		t.Errorf("second pass scrubbed %d rows, want 0 (nothing new aged in)", again)
	}

	// Reaction scrubber: clears reaction text, leaves normal bodies alone.
	rt, err := store.scrubReactionText(ctx, time.Minute)
	if err != nil {
		t.Fatalf("scrubReactionText: %v", err)
	}
	if rt != 1 {
		t.Errorf("scrubReactionText scrubbed %d rows, want 1", rt)
	}
	if got := textOf(t, "tapback-old"); got.Valid && got.String != "" {
		t.Errorf("tapback-old text = %q after reaction scrub, want empty", got.String)
	}
	if got := textOf(t, "undelivered-old"); !got.Valid || got.String == "" {
		t.Errorf("reaction scrub must not touch non-reaction rows")
	}
}

