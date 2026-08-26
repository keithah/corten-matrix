package connector

import (
	"context"
	"errors"
	"testing"
)

func TestWaitForForwardBackfillSlotCancellationSchedulesRetry(t *testing.T) {
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	retryScheduled := false
	err := waitForForwardBackfillSlot(ctx, sem, func() {
		retryScheduled = true
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForForwardBackfillSlot error = %v, want context.Canceled", err)
	}
	if !retryScheduled {
		t.Fatal("canceled semaphore wait did not schedule a forward-backfill retry")
	}
	if len(sem) != 1 {
		t.Fatalf("semaphore occupancy = %d, want 1 (waiting caller must not acquire a slot)", len(sem))
	}
}
