package connector

import "context"

// waitForForwardBackfillSlot acquires a forward-backfill slot or schedules a
// retry before propagating cancellation. The retry callback must not use ctx:
// it runs after the request whose cancellation brought us here has unwound.
func waitForForwardBackfillSlot(ctx context.Context, sem chan struct{}, scheduleRetry func()) error {
	select {
	case sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		scheduleRetry()
		return ctx.Err()
	}
}
