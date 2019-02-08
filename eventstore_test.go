package ehpg_test

import (
	"context"
	"os"
	"testing"

	"github.com/giautm/eh-pg"
	"github.com/go-pg/pg"
	eh "github.com/looplab/eventhorizon"
	testutil "github.com/looplab/eventhorizon/eventstore"
)

func TestEventStore(t *testing.T) {
	db := pg.Connect(&pg.Options{
		Addr:     os.Getenv("POSTGRES_ADDR"),
		Database: os.Getenv("POSTGRES_DB"),
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
	})
	store, err := ehpg.NewEventStore(db)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}
	if store == nil {
		t.Fatal("there should be a store")
	}

	ctx := eh.NewContextWithNamespace(context.Background(), "ns")
	defer store.Close()
	defer func() {
		t.Log("clearing db")
		if err = store.Clear(context.Background()); err != nil {
			t.Fatal("there should be no error:", err)
		}
		if err = store.Clear(ctx); err != nil {
			t.Fatal("there should be no error:", err)
		}
	}()

	// Run the actual test suite.
	t.Log("event store with default namespace")
	testutil.AcceptanceTest(t, context.Background(), store)

	t.Log("event store with other namespace")
	testutil.AcceptanceTest(t, ctx, store)

	t.Log("event store maintainer")
	testutil.MaintainerAcceptanceTest(t, context.Background(), store)
}
