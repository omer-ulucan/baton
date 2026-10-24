package server_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.yaml.in/yaml/v3"
)

// TestOpenAPIDocumentsServedRoutes checks that every operation in
// api/openapi.yaml is routed by the server (not 404/405), so the contract
// cannot drift from the implementation silently.
func TestOpenAPIDocumentsServedRoutes(t *testing.T) {
	raw, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("openapi.yaml does not parse: %v", err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.1") {
		t.Fatalf("openapi version %q", doc.OpenAPI)
	}
	c := newAPI(t)
	n := 0
	for path, ops := range doc.Paths {
		concrete := strings.NewReplacer("{id}", uuid.NewString(), "{task_id}", uuid.NewString()).Replace(path)
		for method := range ops {
			req := httptest.NewRequestWithContext(t.Context(), strings.ToUpper(method), c.url+concrete, strings.NewReader("{}"))
			req.RequestURI = ""
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			// The mux answers unrouted paths with a plain-text 404 or 405;
			// a routed endpoint's own 404 (unknown id) is JSON.
			unrouted := resp.StatusCode == http.StatusMethodNotAllowed ||
				resp.StatusCode == http.StatusNotFound && resp.Header.Get("Content-Type") != "application/json"
			if unrouted {
				t.Errorf("%s %s is documented but not routed (%d)", strings.ToUpper(method), path, resp.StatusCode)
			}
			n++
		}
	}
	if n < 17 {
		t.Fatalf("only %d operations documented", n)
	}
}
