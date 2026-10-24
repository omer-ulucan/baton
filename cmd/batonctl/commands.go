package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/omer-ulucan/baton/internal/api"
)

func (c *cli) print(v any, table func(*tabwriter.Writer)) error {
	if c.json {
		enc := json.NewEncoder(c.out)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
	tw := tabwriter.NewWriter(c.out, 0, 4, 2, ' ', 0)
	table(tw)
	return tw.Flush()
}

func (c *cli) health() error {
	var body struct {
		Status string `json:"status"`
	}
	err := c.api.do(c.ctx, "GET", "/readyz", nil, nil, &body)
	var ae *apiError
	if errors.As(err, &ae) {
		return fmt.Errorf("not ready (%d)", ae.status)
	}
	if err != nil {
		return err
	}
	fmt.Fprintln(c.out, body.Status)
	return nil
}

func (c *cli) submit(args []string) error {
	var file, input, key, callback string
	priority := -1
	pos, err := parse("submit", args, func(fs *flag.FlagSet) {
		fs.StringVar(&file, "f", "", "definition YAML file")
		fs.StringVar(&input, "input", "", "workflow input JSON object")
		fs.IntVar(&priority, "priority", -1, "priority 0..100")
		fs.StringVar(&key, "idempotency-key", "", "client idempotency key")
		fs.StringVar(&callback, "callback", "", "terminal webhook URL")
	})
	if err != nil {
		return err
	}
	if file == "" || len(pos) > 0 {
		return usageError("submit needs -f FILE and no positional arguments")
	}
	def, err := c.readFn(file)
	if err != nil {
		return err
	}
	req := api.SubmitRequest{Definition: string(def), CallbackURL: callback}
	if input != "" {
		if !json.Valid([]byte(input)) {
			return usageError("--input is not valid JSON")
		}
		req.Input = json.RawMessage(input)
	}
	if priority >= 0 {
		req.Priority = &priority
	}
	hdr := map[string]string{}
	if key != "" {
		hdr["Idempotency-Key"] = key
	}
	var resp api.SubmitResponse
	if err := c.api.do(c.ctx, "POST", "/v1/workflows", hdr, req, &resp); err != nil {
		return err
	}
	return c.print(resp, func(tw *tabwriter.Writer) {
		note := ""
		if resp.Existing {
			note = "  (existing submission for this idempotency key)"
		}
		fmt.Fprintf(tw, "%s\t%s%s\n", resp.WorkflowID, resp.Status, note)
	})
}

func (c *cli) status(args []string) error {
	var events bool
	pos, err := parse("status", args, func(fs *flag.FlagSet) { fs.BoolVar(&events, "events", false, "show history") })
	if err != nil {
		return err
	}
	id, err := one("status", pos, "workflow id")
	if err != nil {
		return err
	}
	var wf api.Workflow
	if err := c.api.do(c.ctx, "GET", "/v1/workflows/"+url.PathEscape(id), nil, nil, &wf); err != nil {
		return err
	}
	var hist api.EventList
	if events {
		if err := c.api.do(c.ctx, "GET", "/v1/workflows/"+url.PathEscape(id)+"/events", nil, nil, &hist); err != nil {
			return err
		}
	}
	if c.json {
		if events {
			return c.print(struct {
				api.Workflow
				Events []api.Event `json:"events"`
			}{wf, hist.Events}, nil)
		}
		return c.print(wf, nil)
	}
	return c.print(wf, func(tw *tabwriter.Writer) {
		fmt.Fprintf(tw, "workflow\t%s\nstatus\t%s\npriority\t%d\ncreated\t%s\n", wf.ID, wf.Status, wf.Priority, ts(wf.CreatedAt))
		if wf.FinishedAt != nil {
			fmt.Fprintf(tw, "finished\t%s\n", ts(*wf.FinishedAt))
		}
		fmt.Fprintln(tw, "\nSTEP\tKIND\tSTATUS\tATTEMPT\tQUEUE\tLAST ERROR")
		for _, t := range wf.Tasks {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d/%d\t%s\t%s\n", t.Step, t.Kind, t.Status, t.Attempt, t.MaxAttempts, t.Queue, short(t.LastError))
		}
		if events {
			fmt.Fprintln(tw, "\nSEQ\tTIME\tEVENT\tTASK")
			for _, ev := range hist.Events {
				task := ""
				if ev.TaskID != nil {
					task = ev.TaskID.String()
				}
				fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", ev.Seq, ts(ev.OccurredAt), ev.Type, task)
			}
		}
	})
}

func (c *cli) list(args []string) error {
	var status string
	limit := 50
	if _, err := parse("list", args, func(fs *flag.FlagSet) {
		fs.StringVar(&status, "status", "", "filter by status")
		fs.IntVar(&limit, "limit", 50, "page size")
	}); err != nil {
		return err
	}
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if status != "" {
		q.Set("status", status)
	}
	var resp api.WorkflowList
	if err := c.api.do(c.ctx, "GET", "/v1/workflows?"+q.Encode(), nil, nil, &resp); err != nil {
		return err
	}
	return c.print(resp, func(tw *tabwriter.Writer) {
		fmt.Fprintln(tw, "ID\tSTATUS\tPRIORITY\tCREATED")
		for _, w := range resp.Workflows {
			fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", w.ID, w.Status, w.Priority, ts(w.CreatedAt))
		}
		if resp.NextCursor != "" {
			fmt.Fprintf(tw, "(more: cursor %s)\n", resp.NextCursor)
		}
	})
}

func (c *cli) cancel(args []string) error {
	var req api.CancelRequest
	pos, err := parse("cancel", args, func(fs *flag.FlagSet) {
		fs.BoolVar(&req.Compensate, "compensate", false, "compensate completed steps first")
		fs.StringVar(&req.Reason, "reason", "", "recorded reason")
	})
	if err != nil {
		return err
	}
	id, err := one("cancel", pos, "workflow id")
	if err != nil {
		return err
	}
	return c.statusCall("/v1/workflows/"+url.PathEscape(id)+"/cancel", req)
}

func (c *cli) lifecycle(op string, args []string) error {
	pos, err := parse(op, args, func(*flag.FlagSet) {})
	if err != nil {
		return err
	}
	id, err := one(op, pos, "workflow id")
	if err != nil {
		return err
	}
	return c.statusCall("/v1/workflows/"+url.PathEscape(id)+"/"+op, nil)
}

func (c *cli) statusCall(path string, body any) error {
	var resp api.StatusResponse
	if err := c.api.do(c.ctx, "POST", path, nil, body, &resp); err != nil {
		return err
	}
	return c.print(resp, func(tw *tabwriter.Writer) { fmt.Fprintln(tw, resp.Status) })
}

func (c *cli) dlq(args []string) error {
	if len(args) == 0 {
		return usageError("dlq needs a subcommand: list or retry")
	}
	switch args[0] {
	case "list":
		var queue string
		limit := 50
		if _, err := parse("dlq list", args[1:], func(fs *flag.FlagSet) {
			fs.StringVar(&queue, "queue", "", "filter by queue")
			fs.IntVar(&limit, "limit", 50, "page size")
		}); err != nil {
			return err
		}
		q := url.Values{"limit": {strconv.Itoa(limit)}}
		if queue != "" {
			q.Set("queue", queue)
		}
		var resp api.DLQList
		if err := c.api.do(c.ctx, "GET", "/v1/dlq?"+q.Encode(), nil, nil, &resp); err != nil {
			return err
		}
		return c.print(resp, func(tw *tabwriter.Writer) {
			fmt.Fprintln(tw, "TASK\tWORKFLOW\tSTEP\tQUEUE\tATTEMPTS\tDECISION\tLAST ERROR")
			for _, e := range resp.Entries {
				d := "-"
				if e.LastDecision != nil {
					d = fmt.Sprintf("%s (%s %.2f)", e.LastDecision.Label, e.LastDecision.Policy, e.LastDecision.Confidence)
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n", e.Task.ID, e.WorkflowID, e.Task.Step, e.Task.Queue, e.Task.Attempt, d, short(e.Task.LastError))
			}
		})
	case "retry":
		pos, err := parse("dlq retry", args[1:], func(*flag.FlagSet) {})
		if err != nil {
			return err
		}
		id, err := one("dlq retry", pos, "task id")
		if err != nil {
			return err
		}
		return c.statusCall("/v1/dlq/"+url.PathEscape(id)+"/retry", nil)
	}
	return usageError(fmt.Sprintf("unknown dlq subcommand %q", args[0]))
}

func ts(t time.Time) string { return t.Local().Format(time.DateTime) }

func short(s *string) string {
	if s == nil {
		return ""
	}
	if r := []rune(*s); len(r) > 60 {
		return string(r[:57]) + "..."
	}
	return *s
}
