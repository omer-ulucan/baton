package model

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func pinned(content string) File {
	sum := sha256.Sum256([]byte(content))
	return File{Path: "sub/file.bin", Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])}
}

func hub(t *testing.T, body string, hits *atomic.Int64) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !strings.Contains(r.URL.Path, "/resolve/"+Revision+"/") {
			t.Errorf("download not pinned to the revision: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestFetchVerifiesAndSkipsGoodFiles(t *testing.T) {
	var hits atomic.Int64
	f := pinned("weights")
	dir := t.TempDir()
	fe := &Fetcher{BaseURL: hub(t, "weights", &hits)}
	if err := fe.Fetch(t.Context(), dir, []File{f}); err != nil {
		t.Fatal(err)
	}
	if err := Verify(dir, []File{f}); err != nil {
		t.Fatal(err)
	}
	if err := fe.Fetch(t.Context(), dir, []File{f}); err != nil || hits.Load() != 1 {
		t.Fatalf("second fetch: %v, %d downloads", err, hits.Load())
	}
}

func TestTamperedDownloadIsNeverInstalled(t *testing.T) {
	var hits atomic.Int64
	f := pinned("weights")
	dir := t.TempDir()
	fe := &Fetcher{BaseURL: hub(t, "WEIGHTS", &hits)} // same size, different bytes
	err := fe.Fetch(t.Context(), dir, []File{f})
	if !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "sub"))
	if len(entries) != 0 {
		t.Fatalf("files left behind: %v", entries)
	}
}

func TestCorruptCachedFileIsReplaced(t *testing.T) {
	var hits atomic.Int64
	f := pinned("weights")
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "sub"), 0o755)
	_ = os.WriteFile(filepath.Join(dir, f.Path), []byte("wEights"), 0o600)
	if err := Verify(dir, []File{f}); !errors.Is(err, ErrChecksum) {
		t.Fatalf("corrupt file verified: %v", err)
	}
	if err := (&Fetcher{BaseURL: hub(t, "weights", &hits)}).Fetch(t.Context(), dir, []File{f}); err != nil {
		t.Fatal(err)
	}
	if err := Verify(dir, []File{f}); err != nil {
		t.Fatal(err)
	}
}

func TestManifest(t *testing.T) {
	all, err := Files(FP32, false)
	if err != nil || len(all) != 4 {
		t.Fatalf("fp32 files = %v %v", all, err)
	}
	small, _ := Files(FP32, true)
	for _, f := range small {
		if !f.Small {
			t.Fatalf("small set includes %s", f.Path)
		}
	}
	if _, err := Files("int8", false); err == nil {
		t.Fatal("unknown variant accepted")
	}
	for _, f := range all {
		if len(f.SHA256) != 64 || f.Size <= 0 {
			t.Fatalf("bad manifest entry %+v", f)
		}
	}
	if GraphPath(FP16) != "onnx/model_fp16.onnx" || RevisionLabel(FP32) != Revision+"@fp32" {
		t.Fatal("paths")
	}
}
