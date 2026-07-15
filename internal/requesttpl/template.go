package requesttpl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"text/template"
	"time"
)

type Definition struct {
	Headers      map[string]string
	Query        map[string]string
	BodyEncoding string
	Body         string
}

type Compiled struct {
	headers      map[string]*template.Template
	query        map[string]*template.Template
	bodyEncoding string
	body         *template.Template
}

type EventContext struct {
	EventID    string
	EventType  string
	OccurredAt time.Time
	RoutingKey string
	Payload    map[string]any
}

type Rendered struct {
	Headers      map[string]string
	Query        url.Values
	Body         string
	BodyEncoding string
}

func Compile(name string, d Definition) (*Compiled, error) {
	funcs := template.FuncMap{
		"json": func(v any) (string, error) {
			b, err := json.Marshal(v)
			return string(b), err
		},
		"urlquery": url.QueryEscape,
		"timefmt": func(layout string, v any) (string, error) {
			switch t := v.(type) {
			case time.Time:
				return t.Format(layout), nil
			case string:
				parsed, err := time.Parse(time.RFC3339, t)
				if err != nil {
					return "", err
				}
				return parsed.Format(layout), nil
			default:
				return "", fmt.Errorf("timefmt expects time.Time or RFC3339 string")
			}
		},
		"negate": negate,
	}
	parse := func(suffix, value string) (*template.Template, error) {
		t, err := template.New(name + suffix).Funcs(funcs).Option("missingkey=error").Parse(value)
		if err != nil {
			return nil, fmt.Errorf("compile template %s%s: %w", name, suffix, err)
		}
		return t, nil
	}

	c := &Compiled{
		headers:      make(map[string]*template.Template, len(d.Headers)),
		query:        make(map[string]*template.Template, len(d.Query)),
		bodyEncoding: d.BodyEncoding,
	}
	if c.bodyEncoding == "" {
		c.bodyEncoding = "application/json"
	}
	for k, v := range d.Headers {
		t, err := parse(".header."+k, v)
		if err != nil {
			return nil, err
		}
		c.headers[http.CanonicalHeaderKey(k)] = t
	}
	for k, v := range d.Query {
		t, err := parse(".query."+k, v)
		if err != nil {
			return nil, err
		}
		c.query[k] = t
	}
	var err error
	c.body, err = parse(".body", d.Body)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Compiled) Render(event EventContext) (Rendered, error) {
	ctx := map[string]any{
		"event_id":    event.EventID,
		"event_type":  event.EventType,
		"occurred_at": event.OccurredAt.Format(time.RFC3339Nano),
		"routing_key": event.RoutingKey,
		"payload":     event.Payload,
	}
	exec := func(t *template.Template) (string, error) {
		var out bytes.Buffer
		if err := t.Execute(&out, ctx); err != nil {
			return "", err
		}
		return out.String(), nil
	}

	r := Rendered{Headers: map[string]string{}, Query: url.Values{}, BodyEncoding: c.bodyEncoding}
	if len(c.headers) > 64 {
		return Rendered{}, fmt.Errorf("header count exceeds 64")
	}
	headerBytes := 0
	for name, tmpl := range c.headers {
		value, err := exec(tmpl)
		if err != nil {
			return Rendered{}, fmt.Errorf("render header %s: %w", name, err)
		}
		if strings.ContainsAny(value, "\r\n") {
			return Rendered{}, fmt.Errorf("header %s contains CR/LF", name)
		}
		if len(value) > 8*1024 {
			return Rendered{}, fmt.Errorf("header %s exceeds 8 KiB", name)
		}
		headerBytes += len(name) + len(value)
		if headerBytes > 64*1024 {
			return Rendered{}, fmt.Errorf("rendered headers exceed 64 KiB")
		}
		r.Headers[name] = value
	}
	for name, tmpl := range c.query {
		value, err := exec(tmpl)
		if err != nil {
			return Rendered{}, fmt.Errorf("render query %s: %w", name, err)
		}
		r.Query.Set(name, value)
	}
	body, err := exec(c.body)
	if err != nil {
		return Rendered{}, fmt.Errorf("render body: %w", err)
	}
	if len(body) > 1024*1024 {
		return Rendered{}, fmt.Errorf("rendered body exceeds 1 MiB")
	}
	switch c.bodyEncoding {
	case "application/json":
		var value any
		if err := json.Unmarshal([]byte(body), &value); err != nil {
			return Rendered{}, fmt.Errorf("rendered body is not valid JSON: %w", err)
		}
		canonical, err := json.Marshal(value)
		if err != nil {
			return Rendered{}, fmt.Errorf("canonicalize rendered JSON: %w", err)
		}
		r.Body = string(canonical)
	case "application/x-www-form-urlencoded":
		values, err := url.ParseQuery(body)
		if err != nil {
			return Rendered{}, fmt.Errorf("rendered form body is invalid: %w", err)
		}
		r.Body = values.Encode()
	default:
		return Rendered{}, fmt.Errorf("unsupported body encoding %q", c.bodyEncoding)
	}
	return r, nil
}

func negate(v any) (any, error) {
	switch n := v.(type) {
	case int:
		return -n, nil
	case int64:
		return -n, nil
	case float64:
		return -n, nil
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return -i, nil
		}
		f, err := n.Float64()
		if err != nil {
			return nil, err
		}
		return -f, nil
	default:
		return nil, fmt.Errorf("negate expects a number, got %T", v)
	}
}
