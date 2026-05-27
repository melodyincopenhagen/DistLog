package server

import (
	"embed"
	"io/fs"
)

// consoleFiles holds the static HTML/CSS/JS for the embedded console.
// Files live under internal/server/web/ and are compiled into the
// binary by `go build`. No separate build step or asset pipeline.
//
//go:embed web/*
var consoleFiles embed.FS

// consoleFS is the same set of files rooted at the `web/` subdirectory
// so that browser-visible paths like /console.css resolve to
// web/console.css inside the embed. fs.Sub returns an fs.FS, which
// http.FS adapts to http.FileSystem.
var consoleFS = mustSub(consoleFiles, "web")

func mustSub(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		// Build-time guarantee: web/ exists. A runtime failure here
		// means the embed directive was broken — fail loudly.
		panic("server: console.go: " + err.Error())
	}
	return sub
}
