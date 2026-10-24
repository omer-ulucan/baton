// Package worker is the worker side of the protocol: an HTTP client for
// the orchestrator and a Runner that executes handlers. Workers never
// touch the database.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/omer-ulucan/baton/internal/protocol"
)

// APIError is a non-2xx response from the orchestrator.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("orchestrator: %d %s: %s", e.Status, e.Code, e.Message)
}

// IsCode reports whether err is an APIError with the given code.
func IsCode(err error, code string) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Code == code
}

// Client calls the worker protocol endpoints.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for the orchestrator at base (e.g.
// http://baton-1:8080). hc may be nil for http.DefaultClient.
func NewClient(base string, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{base: strings.TrimRight(base, "/"), http: hc}
}

func (c *Client) Register(ctx context.Context, req protocol.RegisterRequest) (protocol.RegisterResponse, error) {
	var resp protocol.RegisterResponse
	return resp, c.post(ctx, protocol.PathRegister, req, &resp)
}

func (c *Client) Claim(ctx context.Context, req protocol.ClaimRequest) ([]protocol.Task, error) {
	var resp protocol.ClaimResponse
	err := c.post(ctx, protocol.PathClaim, req, &resp)
	return resp.Tasks, err
}

func (c *Client) Heartbeat(ctx context.Context, req protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	var resp protocol.HeartbeatResponse
	return resp, c.post(ctx, protocol.PathHeartbeat, req, &resp)
}

func (c *Client) Complete(ctx context.Context, req protocol.CompleteRequest) error {
	return c.post(ctx, protocol.PathComplete, req, nil)
}

func (c *Client) Fail(ctx context.Context, req protocol.FailRequest) (protocol.FailResponse, error) {
	var resp protocol.FailResponse
	return resp, c.post(ctx, protocol.PathFail, req, &resp)
}

func (c *Client) Release(ctx context.Context, req protocol.ReleaseRequest) error {
	return c.post(ctx, protocol.PathRelease, req, nil)
}

func (c *Client) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		var eb protocol.ErrorBody
		_ = json.Unmarshal(data, &eb)
		return &APIError{Status: resp.StatusCode, Code: eb.Error.Code, Message: eb.Error.Message}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}
