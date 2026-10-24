package store_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/omer-ulucan/baton/internal/pgtest"
	"github.com/omer-ulucan/baton/internal/store"
)

var pg *pgtest.Server

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Short() {
		ctx := context.Background()
		var err error
		if pg, err = pgtest.Start(ctx, store.Migrate); err != nil {
			fmt.Fprintln(os.Stderr, "pgtest:", err)
			os.Exit(1)
		}
		code := m.Run()
		_ = pg.Close(ctx)
		os.Exit(code)
	}
	os.Exit(m.Run())
}
