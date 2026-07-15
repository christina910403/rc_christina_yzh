package config

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"gopkg.in/yaml.v3"

	"github.com/christina910403/rc_christina_yzh/internal/domain"
	"github.com/christina910403/rc_christina_yzh/internal/requesttpl"
)

type File struct {
	Server          ServerConfig      `yaml:"server"`
	Worker          WorkerConfig      `yaml:"worker"`
	Security        SecurityConfig    `yaml:"security"`
	Sources         []Source          `yaml:"sources"`
	Events          []EventDefinition `yaml:"events"`
	ExternalSystems []ExternalSystem  `yaml:"external_systems"`
	APIOperations   []APIOperation    `yaml:"api_operations"`
	EventRoutes     []EventRoute      `yaml:"event_routes"`
}

type ServerConfig struct {
	Address      string `yaml:"address"`
	MaxBodyBytes int64  `yaml:"max_body_bytes"`
	OpsAPIKeyEnv string `yaml:"ops_api_key_env"`
}

type WorkerConfig struct {
	PollIntervalMS int      `yaml:"poll_interval_ms"`
	BatchSize      int      `yaml:"batch_size"`
	LeaseSeconds   int      `yaml:"lease_seconds"`
	MaxAttempts    int      `yaml:"max_attempts"`
	RetryDelays    []string `yaml:"retry_delays"`
}

type SecurityConfig struct {
	AllowInsecureHTTP   bool `yaml:"allow_insecure_http"`
	AllowPrivateNetwork bool `yaml:"allow_private_network"`
}

type Source struct {
	ID        string `yaml:"id"`
	APIKeyEnv string `yaml:"api_key_env"`
}

type EventDefinition struct {
	ID           string `yaml:"id"`
	Source       string `yaml:"source"`
	Schema       string `yaml:"schema"`
	RequireRoute bool   `yaml:"require_route"`
}

type ExternalSystem struct {
	ID                 string `yaml:"id"`
	BaseURL            string `yaml:"base_url"`
	MaxConcurrency     int    `yaml:"max_concurrency"`
	RateLimitPerSecond int    `yaml:"rate_limit_per_second"`
}

type APIOperation struct {
	ID                  string            `yaml:"id"`
	Revision            int               `yaml:"revision"`
	ExternalSystem      string            `yaml:"external_system"`
	Method              string            `yaml:"method"`
	Path                string            `yaml:"path"`
	StaticHeaders       map[string]string `yaml:"static_headers"`
	SecretHeaders       map[string]string `yaml:"secret_headers"`
	AllowedRouteHeaders []string          `yaml:"allowed_route_headers"`
	IdempotencyHeader   string            `yaml:"idempotency_header"`
	TimeoutMS           int               `yaml:"request_timeout_ms"`
	MaxAttempts         int               `yaml:"max_attempts"`
	RetryDelays         []string          `yaml:"retry_delays"`
}

type EventRoute struct {
	ID              string            `yaml:"id"`
	Revision        int               `yaml:"revision"`
	EventType       string            `yaml:"event_type"`
	RoutingKey      string            `yaml:"routing_key"`
	APIOperation    string            `yaml:"api_operation"`
	ValidFrom       string            `yaml:"valid_from"`
	ValidUntil      string            `yaml:"valid_until"`
	HeaderTemplates map[string]string `yaml:"header_templates"`
	QueryTemplates  map[string]string `yaml:"query_templates"`
	BodyEncoding    string            `yaml:"body_encoding"`
	BodyTemplate    string            `yaml:"body_template"`
	Enabled         bool              `yaml:"enabled"`
}

type Runtime struct {
	File          File
	Version       string
	Events        map[string]*RuntimeEvent
	RoutesByEvent map[string][]*RuntimeRoute
	Operations    map[string]*RuntimeOperation
	sources       []Source
}

type RuntimeEvent struct {
	Definition EventDefinition
	Schema     *jsonschema.Schema
}

type RuntimeOperation struct {
	Definition     APIOperation
	ExternalSystem ExternalSystem
	URL            string
	AllowedHeaders map[string]struct{}
	RetryDelaysMS  []int64
}

type RuntimeRoute struct {
	Definition EventRoute
	Operation  *RuntimeOperation
	Template   *requesttpl.Compiled
	ValidFrom  *time.Time
	ValidUntil *time.Time
}

func Load(path string) (*Runtime, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var f File
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&f)
	rt := &Runtime{
		File:          f,
		Events:        map[string]*RuntimeEvent{},
		RoutesByEvent: map[string][]*RuntimeRoute{},
		Operations:    map[string]*RuntimeOperation{},
		sources:       f.Sources,
	}
	configHasher := sha256.New()
	_, _ = configHasher.Write(raw)
	baseDir := filepath.Dir(path)

	sources := map[string]struct{}{}
	for _, source := range f.Sources {
		if source.ID == "" || source.APIKeyEnv == "" {
			return nil, fmt.Errorf("every source requires id and api_key_env")
		}
		if _, exists := sources[source.ID]; exists {
			return nil, fmt.Errorf("duplicate source %q", source.ID)
		}
		sources[source.ID] = struct{}{}
	}

	for _, event := range f.Events {
		if event.ID == "" || event.Source == "" || event.Schema == "" {
			return nil, fmt.Errorf("every event requires id, source and schema")
		}
		if _, ok := sources[event.Source]; !ok {
			return nil, fmt.Errorf("event %q references unknown source %q", event.ID, event.Source)
		}
		if _, exists := rt.Events[event.ID]; exists {
			return nil, fmt.Errorf("duplicate event %q", event.ID)
		}
		schemaPath := event.Schema
		if !filepath.IsAbs(schemaPath) {
			schemaPath = filepath.Join(baseDir, schemaPath)
		}
		abs, err := filepath.Abs(schemaPath)
		if err != nil {
			return nil, fmt.Errorf("resolve schema for %s: %w", event.ID, err)
		}
		schemaBytes, err := os.ReadFile(abs)
		if err != nil {
			return nil, fmt.Errorf("read schema for %s: %w", event.ID, err)
		}
		_, _ = configHasher.Write(schemaBytes)
		compiler := jsonschema.NewCompiler()
		schema, err := compiler.Compile("file://" + filepath.ToSlash(abs))
		if err != nil {
			return nil, fmt.Errorf("compile schema for %s: %w", event.ID, err)
		}
		rt.Events[event.ID] = &RuntimeEvent{Definition: event, Schema: schema}
	}

	systems := map[string]ExternalSystem{}
	for _, system := range f.ExternalSystems {
		if system.ID == "" || system.BaseURL == "" {
			return nil, fmt.Errorf("every external system requires id and base_url")
		}
		if _, exists := systems[system.ID]; exists {
			return nil, fmt.Errorf("duplicate external system %q", system.ID)
		}
		if _, err := validateBaseURL(system.BaseURL, f.Security); err != nil {
			return nil, fmt.Errorf("external system %s: %w", system.ID, err)
		}
		systems[system.ID] = system
	}

	defaultDelays, err := parseDurations(f.Worker.RetryDelays)
	if err != nil {
		return nil, fmt.Errorf("worker retry_delays: %w", err)
	}
	for _, op := range f.APIOperations {
		if op.ID == "" || op.ExternalSystem == "" || op.Method == "" {
			return nil, fmt.Errorf("every api operation requires id, external_system and method")
		}
		if _, exists := rt.Operations[op.ID]; exists {
			return nil, fmt.Errorf("duplicate api operation %q", op.ID)
		}
		system, ok := systems[op.ExternalSystem]
		if !ok {
			return nil, fmt.Errorf("api operation %q references unknown system %q", op.ID, op.ExternalSystem)
		}
		op.StaticHeaders = canonicalHeaders(op.StaticHeaders)
		op.SecretHeaders = canonicalHeaders(op.SecretHeaders)
		op.IdempotencyHeader = http.CanonicalHeaderKey(op.IdempotencyHeader)
		if op.TimeoutMS == 0 {
			op.TimeoutMS = 10000
		}
		method := strings.ToUpper(op.Method)
		if method != http.MethodPost && method != http.MethodPut && method != http.MethodPatch {
			return nil, fmt.Errorf("api operation %q uses unsupported method %q", op.ID, op.Method)
		}
		op.Method = method
		base, _ := url.Parse(system.BaseURL)
		rel, err := url.Parse(op.Path)
		if err != nil {
			return nil, fmt.Errorf("api operation %q path: %w", op.ID, err)
		}
		if rel.IsAbs() || rel.Host != "" || !strings.HasPrefix(rel.Path, "/") {
			return nil, fmt.Errorf("api operation %q path must be an absolute path without scheme or host", op.ID)
		}
		finalURL := base.ResolveReference(rel).String()
		allowed := map[string]struct{}{}
		for _, name := range op.AllowedRouteHeaders {
			canonical := http.CanonicalHeaderKey(name)
			if err := validateHeaderName(canonical); err != nil {
				return nil, fmt.Errorf("api operation %q: %w", op.ID, err)
			}
			if isSensitiveHeader(canonical) {
				return nil, fmt.Errorf("api operation %q cannot allow route to set sensitive header %q", op.ID, canonical)
			}
			if canonical == op.IdempotencyHeader {
				return nil, fmt.Errorf("api operation %q cannot allow route to set idempotency header %q", op.ID, canonical)
			}
			allowed[canonical] = struct{}{}
		}
		if err := validateOperationHeaders(op); err != nil {
			return nil, fmt.Errorf("api operation %q: %w", op.ID, err)
		}
		if op.TimeoutMS > f.Worker.LeaseSeconds*1000 {
			return nil, fmt.Errorf("api operation %q timeout must not exceed worker lease", op.ID)
		}
		delays := defaultDelays
		if len(op.RetryDelays) > 0 {
			delays, err = parseDurations(op.RetryDelays)
			if err != nil {
				return nil, fmt.Errorf("api operation %q retry_delays: %w", op.ID, err)
			}
		}
		rt.Operations[op.ID] = &RuntimeOperation{
			Definition: op, ExternalSystem: system, URL: finalURL,
			AllowedHeaders: allowed, RetryDelaysMS: delays,
		}
	}

	routeIDs := map[string]struct{}{}
	for _, route := range f.EventRoutes {
		if route.ID == "" || route.EventType == "" || route.APIOperation == "" {
			return nil, fmt.Errorf("every event route requires id, event_type and api_operation")
		}
		if _, exists := routeIDs[route.ID]; exists {
			return nil, fmt.Errorf("duplicate event route %q", route.ID)
		}
		routeIDs[route.ID] = struct{}{}
		if _, ok := rt.Events[route.EventType]; !ok {
			return nil, fmt.Errorf("route %q references unknown event %q", route.ID, route.EventType)
		}
		op, ok := rt.Operations[route.APIOperation]
		if !ok {
			return nil, fmt.Errorf("route %q references unknown operation %q", route.ID, route.APIOperation)
		}
		for name := range route.HeaderTemplates {
			canonical := http.CanonicalHeaderKey(name)
			if isSensitiveHeader(canonical) {
				return nil, fmt.Errorf("route %q cannot set sensitive header %q", route.ID, name)
			}
			if _, ok := op.AllowedHeaders[canonical]; !ok {
				return nil, fmt.Errorf("route %q cannot set header %q", route.ID, name)
			}
			if _, exists := op.Definition.StaticHeaders[canonical]; exists {
				return nil, fmt.Errorf("route %q header %q conflicts with static header", route.ID, name)
			}
			if _, exists := op.Definition.SecretHeaders[canonical]; exists {
				return nil, fmt.Errorf("route %q header %q conflicts with secret header", route.ID, name)
			}
		}
		compiled, err := requesttpl.Compile(route.ID, requesttpl.Definition{
			Headers: route.HeaderTemplates, Query: route.QueryTemplates,
			BodyEncoding: route.BodyEncoding, Body: route.BodyTemplate,
		})
		if err != nil {
			return nil, err
		}
		from, err := parseOptionalTime(route.ValidFrom)
		if err != nil {
			return nil, fmt.Errorf("route %q valid_from: %w", route.ID, err)
		}
		until, err := parseOptionalTime(route.ValidUntil)
		if err != nil {
			return nil, fmt.Errorf("route %q valid_until: %w", route.ID, err)
		}
		if from != nil && until != nil && !from.Before(*until) {
			return nil, fmt.Errorf("route %q valid_from must be before valid_until", route.ID)
		}
		rt.RoutesByEvent[route.EventType] = append(rt.RoutesByEvent[route.EventType], &RuntimeRoute{
			Definition: route, Operation: op, Template: compiled, ValidFrom: from, ValidUntil: until,
		})
	}
	for _, routes := range rt.RoutesByEvent {
		sort.Slice(routes, func(i, j int) bool { return routes[i].Definition.ID < routes[j].Definition.ID })
	}
	rt.Version = hex.EncodeToString(configHasher.Sum(nil))
	return rt, nil
}

func (r *Runtime) Authenticate(apiKey string) (string, bool) {
	for _, source := range r.sources {
		expected := os.Getenv(source.APIKeyEnv)
		if expected == "" || len(expected) != len(apiKey) {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(expected), []byte(apiKey)) == 1 {
			return source.ID, true
		}
	}
	return "", false
}

func (r *Runtime) AuthenticateOps(apiKey string) bool {
	expected := os.Getenv(r.File.Server.OpsAPIKeyEnv)
	return expected != "" && len(expected) == len(apiKey) && subtle.ConstantTimeCompare([]byte(expected), []byte(apiKey)) == 1
}

func (r *Runtime) ValidateEvent(source string, event domain.EventInput) (*RuntimeEvent, error) {
	definition, ok := r.Events[event.EventType]
	if !ok {
		return nil, fmt.Errorf("event type %q is not configured", event.EventType)
	}
	if definition.Definition.Source != source {
		return nil, fmt.Errorf("source %q cannot publish event type %q", source, event.EventType)
	}
	if err := definition.Schema.Validate(event.Payload); err != nil {
		return nil, fmt.Errorf("payload violates schema: %w", err)
	}
	return definition, nil
}

func (r *Runtime) MatchRoutes(event domain.EventInput) []*RuntimeRoute {
	var matched []*RuntimeRoute
	for _, route := range r.RoutesByEvent[event.EventType] {
		if !route.Definition.Enabled {
			continue
		}
		routingKey := route.Definition.RoutingKey
		if routingKey == "" {
			routingKey = "default"
		}
		if event.RoutingKey != routingKey {
			continue
		}
		if route.ValidFrom != nil && event.OccurredAt.Before(*route.ValidFrom) {
			continue
		}
		if route.ValidUntil != nil && !event.OccurredAt.Before(*route.ValidUntil) {
			continue
		}
		matched = append(matched, route)
	}
	return matched
}

func applyDefaults(f *File) {
	if f.Server.Address == "" {
		f.Server.Address = ":8080"
	}
	if f.Server.MaxBodyBytes == 0 {
		f.Server.MaxBodyBytes = 1024 * 1024
	}
	if f.Worker.PollIntervalMS == 0 {
		f.Worker.PollIntervalMS = 1000
	}
	if f.Worker.BatchSize == 0 {
		f.Worker.BatchSize = 50
	}
	if f.Worker.LeaseSeconds == 0 {
		f.Worker.LeaseSeconds = 60
	}
	if f.Worker.MaxAttempts == 0 {
		f.Worker.MaxAttempts = 7
	}
	if len(f.Worker.RetryDelays) == 0 {
		f.Worker.RetryDelays = []string{"1m", "5m", "30m", "2h", "6h", "24h"}
	}
}

func parseDurations(values []string) ([]int64, error) {
	result := make([]int64, 0, len(values))
	for _, value := range values {
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid duration %q", value)
		}
		result = append(result, d.Milliseconds())
	}
	return result, nil
}

func parseOptionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func validateBaseURL(value string, security SecurityConfig) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil {
		return nil, err
	}
	if u.User != nil {
		return nil, fmt.Errorf("URL credentials are forbidden")
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("URL requires a host")
	}
	if u.Scheme != "https" && !(security.AllowInsecureHTTP && u.Scheme == "http") {
		return nil, fmt.Errorf("URL must use HTTPS")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("base_url cannot include query or fragment")
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil && !security.AllowPrivateNetwork && forbiddenIP(ip) {
		return nil, fmt.Errorf("private or special-use IP is forbidden")
	}
	return u, nil
}

func validateOperationHeaders(op APIOperation) error {
	seen := map[string]string{}
	for kind, headers := range map[string]map[string]string{"static": op.StaticHeaders, "secret": op.SecretHeaders} {
		for name, value := range headers {
			canonical := http.CanonicalHeaderKey(name)
			if err := validateHeaderName(canonical); err != nil {
				return err
			}
			if previous, ok := seen[canonical]; ok {
				return fmt.Errorf("header %q exists in %s and %s headers", canonical, previous, kind)
			}
			seen[canonical] = kind
			if kind == "static" && isSensitiveHeader(canonical) {
				return fmt.Errorf("sensitive header %q must be configured as a secret header", canonical)
			}
			if strings.ContainsAny(value, "\r\n") {
				return fmt.Errorf("header %q contains CR/LF", canonical)
			}
			if kind == "secret" && !strings.HasPrefix(value, "env:") {
				return fmt.Errorf("secret header %q must use env: reference", canonical)
			}
		}
	}
	if op.IdempotencyHeader != "" {
		canonical := http.CanonicalHeaderKey(op.IdempotencyHeader)
		if err := validateHeaderName(canonical); err != nil {
			return err
		}
		if _, ok := seen[canonical]; ok {
			return fmt.Errorf("idempotency header %q conflicts with configured header", canonical)
		}
	}
	return nil
}

func validateHeaderName(name string) error {
	if name == "" {
		return fmt.Errorf("header name cannot be empty")
	}
	forbidden := map[string]struct{}{
		"Host": {}, "Content-Length": {}, "Connection": {}, "Transfer-Encoding": {},
	}
	if _, ok := forbidden[name]; ok {
		return fmt.Errorf("header %q is platform-reserved", name)
	}
	return nil
}

func canonicalHeaders(headers map[string]string) map[string]string {
	result := make(map[string]string, len(headers))
	for name, value := range headers {
		result[http.CanonicalHeaderKey(name)] = value
	}
	return result
}

func isSensitiveHeader(name string) bool {
	lower := strings.ToLower(name)
	return strings.Contains(lower, "authorization") || strings.Contains(lower, "cookie") ||
		strings.Contains(lower, "api-key") || strings.Contains(lower, "token") || strings.Contains(lower, "secret")
}

func forbiddenIP(ip net.IP) bool {
	return ip == nil || ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast()
}
