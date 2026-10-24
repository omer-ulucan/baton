package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/omer-ulucan/baton/internal/api"
)

type client struct {
	base string
	http *http.Client
}

func newClient(base string) *client {
	return &client{base: strings.TrimRight(base, "/"), http: &http.Client{Timeout: 30 * time.Second}}
}

// apiError is a non-2xx answer.
type apiError struct {
	status int
	body   api.ErrorBody
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%d %s: %s", e.status, e.body.Error.Code, e.body.Error.Message)
	if ps, ok := e.body.Error.Details["problems"].([]any); ok {
		for _, p := range ps {
			msg += fmt.Sprintf("\n  - %v", p)
		}
	}
	return msg
}

func (c *client) do(ctx context.Context, method, path string, hdr map[string]string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		e := &apiError{status: resp.StatusCode}
		if json.Unmarshal(data, &e.body) != nil || e.body.Error.Code == "" {
			e.body.Error.Message = strings.TrimSpace(string(data))
		}
		return e
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
