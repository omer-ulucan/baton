// Package migrations embeds the SQL schema migrations.
package migrations

import "embed"

// FS holds the golang-migrate files (NNNNNN_name.up.sql / .down.sql).
//
//go:embed *.sql
var FS embed.FS
