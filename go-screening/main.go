// Command go-screening is a drop-in re-implementation of the Phase 5 Python
// screening worker (mirrors the real Exness service language). Same topics,
// same DB, same golden contract: consume profile.normalized, query the provider
// gateway, fuzzy-match, persist screening + match, produce screening.completed,
// emit the structured `screen` log line. Idempotent on trace_id:topic:partition:offset.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/segmentio/kafka-go"
)

const (
	component       = "screening-worker-go"
	topicIn         = "profile.normalized"
	topicOut        = "screening.completed"
	consumerGroup   = "screening-worker"
)

type providerCfg struct {
	providerID string
	listType   string
	baseURL    string
}

type candidate struct {
	EntryID     string                 `json:"entry_id"`
	ProviderID  string                 `json:"provider_id"`
	ListType    string                 `json:"list_type"`
	ListVersion string                 `json:"list_version"`
	EntityName  string                 `json:"entity_name"`
	Aliases     []string               `json:"aliases"`
	DOBIso      string                 `json:"dob_iso"`
	CountryIso2 string                 `json:"country_iso2"`
	RiskPayload map[string]interface{} `json:"risk_payload"`
}

type searchResponse struct {
	ProviderID  string      `json:"provider_id"`
	ListType    string      `json:"list_type"`
	ListVersion string      `json:"list_version"`
	Candidates  []candidate `json:"candidates"`
}

type envelope struct {
	TraceID   string          `json:"trace_id"`
	ClientID  string          `json:"client_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type normalizedPayload struct {
	ProfileHash   string `json:"profile_hash"`
	CanonicalName string `json:"canonical_name"`
	DOBIso        string `json:"dob_iso"`
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func providers() []providerCfg {
	return []providerCfg{
		{"world_check", "sanctions", env("AML_WORLD_CHECK_URL", "http://localhost:9101")},
		{"dow_jones", "pep", env("AML_DOW_JONES_URL", "http://localhost:9102")},
		{"comply_advantage", "adverse_media", env("AML_COMPLY_ADVANTAGE_URL", "http://localhost:9103")},
	}
}

func databaseURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		env("AML_POSTGRES_USER", "aml"), env("AML_POSTGRES_PASSWORD", "aml_secret"),
		env("AML_POSTGRES_HOST", "localhost"), env("AML_POSTGRES_PORT", "5432"),
		env("AML_POSTGRES_DB", "aml_sentinel"))
}

func newID() string {
	return ulid.Make().String()
}

func logLine(fields map[string]interface{}) {
	fields["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	fields["component"] = component
	b, _ := json.Marshal(fields)
	fmt.Println(string(b))
}

// queryProvider calls a mock's /search and returns its candidates + list_version.
func queryProvider(client *http.Client, cfg providerCfg, name, dob string) (searchResponse, error) {
	q := url.Values{}
	q.Set("name", name)
	q.Set("limit", "50")
	if dob != "" {
		q.Set("dob", dob)
	}
	resp, err := client.Get(cfg.baseURL + "/search?" + q.Encode())
	if err != nil {
		return searchResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return searchResponse{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var sr searchResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return searchResponse{}, fmt.Errorf("malformed: %w", err)
	}
	return sr, nil
}

type matchOut struct {
	MatchID         string      `json:"match_id"`
	ProviderID      string      `json:"provider_id"`
	ListType        string      `json:"list_type"`
	MatchedName     string      `json:"matched_name"`
	Score           float64     `json:"score"`
	DOBMatch        bool        `json:"dob_match"`
	EvidenceRef     string      `json:"evidence_ref"`
	PEPTier         interface{} `json:"pep_tier"`
	MediaConfidence interface{} `json:"media_confidence"`
}

func processMessage(ctx context.Context, pool *pgxpool.Pool, httpClient *http.Client,
	writer *kafka.Writer, env envelope, topic string, partition int, offset int64) error {

	key := fmt.Sprintf("%s:%s:%d:%d", env.TraceID, topic, partition, offset)
	var exists bool
	if err := pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM idempotency WHERE key=$1)", key).Scan(&exists); err != nil {
		return err
	}
	if exists {
		logLine(map[string]interface{}{"level": "info", "stage": "screen", "status": "ok",
			"trace_id": env.TraceID, "client_id": env.ClientID,
			"detail": map[string]interface{}{"idempotent_skip": true, "key": key}})
		return nil
	}

	var p normalizedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return err
	}

	started := time.Now()
	screeningID := newID()
	listVersions := map[string]string{}
	matches := []matchOut{}
	candidatesTotal := 0
	maxScore := 0.0

	for _, cfg := range providers() {
		sr, err := queryProvider(httpClient, cfg, p.CanonicalName, p.DOBIso)
		if err != nil {
			// Degraded provider: skip its candidates (contract: no crash).
			listVersions[cfg.providerID] = "unknown"
			continue
		}
		listVersions[cfg.providerID] = sr.ListVersion
		candidatesTotal += len(sr.Candidates)
		for _, c := range sr.Candidates {
			sc := scoreCandidate(p.CanonicalName, p.DOBIso, c.EntityName, c.Aliases, c.DOBIso)
			if sc.Score >= ScreeningThreshold {
				var pepTier, mediaConf interface{}
				if c.RiskPayload != nil {
					pepTier = c.RiskPayload["pep_tier"]
					mediaConf = c.RiskPayload["media_confidence"]
				}
				matches = append(matches, matchOut{
					MatchID: newID(), ProviderID: c.ProviderID, ListType: c.ListType,
					MatchedName: sc.MatchedName, Score: sc.Score, DOBMatch: sc.DOBMatch,
					EvidenceRef: c.EntryID, PEPTier: pepTier, MediaConfidence: mediaConf,
				})
				if sc.Score > maxScore {
					maxScore = sc.Score
				}
			}
		}
	}

	// Persist screening + matches + idempotency atomically (parent first for FK).
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	lvJSON, _ := json.Marshal(listVersions)
	if _, err := tx.Exec(ctx,
		`INSERT INTO screening (screening_id, client_id, trace_id, profile_hash, list_versions, status, screened_at)
		 VALUES ($1,$2,$3,$4,$5::jsonb,'completed',now())`,
		screeningID, env.ClientID, env.TraceID, p.ProfileHash, string(lvJSON)); err != nil {
		return err
	}
	for _, m := range matches {
		if _, err := tx.Exec(ctx,
			`INSERT INTO match (match_id, screening_id, provider_id, list_type, matched_name, score, dob_match, created_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,now())`,
			m.MatchID, screeningID, m.ProviderID, m.ListType, m.MatchedName, m.Score, m.DOBMatch); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, "INSERT INTO idempotency (key, processed_at) VALUES ($1, now())", key); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Produce screening.completed.
	payload := map[string]interface{}{
		"screening_id": screeningID, "profile_hash": p.ProfileHash,
		"list_versions": listVersions, "matches": matches,
	}
	out := map[string]interface{}{
		"trace_id": env.TraceID, "client_id": env.ClientID, "event_type": topicOut,
		"schema_version": 1, "produced_at": time.Now().UTC().Format(time.RFC3339Nano),
		"producer": component, "payload": payload,
	}
	value, _ := json.Marshal(out)
	if err := writer.WriteMessages(ctx, kafka.Message{
		Key: []byte(env.ClientID), Value: value, Topic: topicOut,
	}); err != nil {
		return err
	}

	logLine(map[string]interface{}{"level": "info", "stage": "screen", "status": "ok",
		"trace_id": env.TraceID, "client_id": env.ClientID,
		"duration_ms": time.Since(started).Milliseconds(),
		"detail": map[string]interface{}{"screening_id": screeningID,
			"providers_queried": len(providers()), "candidates": candidatesTotal,
			"matches": len(matches), "max_score": round4(maxScore)}})
	return nil
}

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	brokers := strings.Split(env("AML_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ",")
	httpClient := &http.Client{Timeout: 2 * time.Second}
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll}
	defer writer.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers, GroupID: env("AML_GO_GROUP", consumerGroup), Topic: topicIn,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	logLine(map[string]interface{}{"level": "info", "stage": "screen", "status": "ok",
		"trace_id": "-", "client_id": "-", "detail": map[string]interface{}{"started": true}})

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			logLine(map[string]interface{}{"level": "error", "stage": "screen", "status": "failed",
				"trace_id": "-", "client_id": "-",
				"detail": map[string]interface{}{"error": err.Error()}})
			time.Sleep(time.Second)
			continue
		}
		var e envelope
		if err := json.Unmarshal(msg.Value, &e); err == nil {
			if perr := processMessage(ctx, pool, httpClient, writer, e, msg.Topic, msg.Partition, msg.Offset); perr != nil {
				logLine(map[string]interface{}{"level": "error", "stage": "screen", "status": "failed",
					"trace_id": e.TraceID, "client_id": e.ClientID,
					"detail": map[string]interface{}{"error": perr.Error()}})
			}
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			logLine(map[string]interface{}{"level": "error", "stage": "screen", "status": "failed",
				"trace_id": "-", "client_id": "-",
				"detail": map[string]interface{}{"commit_error": err.Error()}})
		}
	}
}

// _ keeps strconv imported if future numeric parsing is added.
var _ = strconv.Itoa
