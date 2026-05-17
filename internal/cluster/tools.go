//go:build tools
// +build tools

// Package cluster blank-imports build-time dependencies that are not yet
// referenced by application code in this branch. Without this file,
// `go mod tidy` would strip the requires from go.mod even though Tasks 4-6
// will import them. Remove this file when all three packages are imported
// by real application code in this package tree.
package cluster

import (
	_ "github.com/hashicorp/memberlist"
	_ "github.com/hashicorp/raft"
	_ "github.com/hashicorp/raft-boltdb/v2"
)
