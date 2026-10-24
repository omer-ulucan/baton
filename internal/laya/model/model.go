// Package model pins, downloads and verifies the Laya ONNX artifact
// (ADR 0008). Files come from one exact Hugging Face revision and are
// accepted only if their SHA-256 matches the manifest; they are never
// committed to the repository.
package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// Repo and Revision identify the pinned export.
const (
	Repo     = "onnx-community/laya-ONNX"
	Revision = "42e2a6e3b3708c8ff5f61f4aa75c314b1449199b"
)

// Variant selects the weights.
type Variant string

const (
	FP32 Variant = "fp32" // default; matches the reference within 1e-4
	FP16 Variant = "fp16" // half the size; max |Δp| ~0.007 per the model card
)

// File is one pinned artifact.
type File struct {
	Path   string // path in the repository, also under the cache dir
	Size   int64
	SHA256 string
	Small  bool // needed by the tokenizer and config, not the weights
}

// Common files are needed for every variant. SHA-256s of the LFS files are
// the Hub's LFS object ids; those of tokenizer.json and config.json were
// computed on files whose git blob ids matched the Hub's tree listing.
var common = []File{
	{Path: "config.json", Size: 3281, SHA256: "1a888f1373542026f6e09b4bdfa746eb6ed6cd0713187cf18c5602e92993bb0c", Small: true},
	{Path: "tokenizer.json", Size: 3583228, SHA256: "6c8aaa9a542084f2457eab775d4eeb51f92a70c0fd9de28d5edb0ddec3c08d30", Small: true},
}

var weights = map[Variant][]File{
	FP32: {
		{Path: "onnx/model.onnx", Size: 4442939, SHA256: "04ac226b2ce6cfd35a28e8cefcc897e9fcc2901b0bfa86d0f518d5b94491f7b0"},
		{Path: "onnx/model.onnx_data", Size: 1685175296, SHA256: "afcc11e72b59193fbec6e5dc8b693c9baa81cf66bec53279cefd687b318c3199"},
	},
	FP16: {
		{Path: "onnx/model_fp16.onnx", Size: 4466054, SHA256: "027403a80579f1546bac9831383c00ab9469a4e7c37aa8f68fdcecd85f8ef29e"},
		{Path: "onnx/model_fp16.onnx_data", Size: 842587136, SHA256: "6631bd51ee258787336568fda528ae3534fb5a3e13386558e7502043cbff4828"},
	},
}

// Files returns the files a variant needs; with small, only the tokenizer
// and config.
func Files(v Variant, small bool) ([]File, error) {
	w, ok := weights[v]
	if !ok {
		return nil, fmt.Errorf("unknown model variant %q (want fp32 or fp16)", v)
	}
	if small {
		return common, nil
	}
	return append(append([]File(nil), common...), w...), nil
}

// GraphPath is the ONNX graph of a variant, relative to the model dir.
func GraphPath(v Variant) string { return weights[v][0].Path }

// RevisionLabel is what DecisionMade records as model_revision.
func RevisionLabel(v Variant) string { return Revision + "@" + string(v) }

// DefaultDir is $XDG_CACHE_HOME/baton/models/<revision> (or ~/.cache/...).
func DefaultDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "baton", "models", Revision), nil
}

// ErrChecksum means a file's content does not match the manifest.
var ErrChecksum = errors.New("model file checksum mismatch")

// Verify checks that every file is present with the pinned size and hash.
func Verify(dir string, files []File) error {
	for _, f := range files {
		if err := verifyFile(filepath.Join(dir, f.Path), f); err != nil {
			return err
		}
	}
	return nil
}

func verifyFile(path string, f File) error {
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fh.Close()
	st, err := fh.Stat()
	if err != nil {
		return err
	}
	if st.Size() != f.Size {
		return fmt.Errorf("%w: %s is %d bytes, want %d", ErrChecksum, f.Path, st.Size(), f.Size)
	}
	h := sha256.New()
	if _, err := io.Copy(h, fh); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return fmt.Errorf("%w: %s has sha256 %s, want %s", ErrChecksum, f.Path, got, f.SHA256)
	}
	return nil
}

// Fetcher downloads pinned files.
type Fetcher struct {
	HTTP    *http.Client
	BaseURL string // default https://huggingface.co
	Log     func(format string, args ...any)
}

// Fetch makes sure every file is in dir and verified, downloading what is
// missing or wrong. Each download is hashed while it streams to a
// temporary file and renamed into place only if it matches, so a dir never
// holds a partial or tampered file under its final name.
func (fe *Fetcher) Fetch(ctx context.Context, dir string, files []File) error {
	for _, f := range files {
		dst := filepath.Join(dir, f.Path)
		if err := verifyFile(dst, f); err == nil {
			continue
		}
		if fe.Log != nil {
			fe.Log("downloading %s (%d MB)", f.Path, f.Size>>20)
		}
		if err := fe.download(ctx, dst, f); err != nil {
			return fmt.Errorf("fetch %s: %w", f.Path, err)
		}
	}
	return nil
}

func (fe *Fetcher) download(ctx context.Context, dst string, f File) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	base := fe.BaseURL
	if base == "" {
		base = "https://huggingface.co"
	}
	url := strings.TrimRight(base, "/") + "/" + Repo + "/resolve/" + Revision + "/" + f.Path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	hc := fe.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".*.part")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(resp.Body, f.Size+1))
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if n != f.Size {
		return fmt.Errorf("%w: downloaded %d bytes, want %d", ErrChecksum, n, f.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return fmt.Errorf("%w: downloaded sha256 %s, want %s", ErrChecksum, got, f.SHA256)
	}
	return os.Rename(tmp.Name(), dst)
}
