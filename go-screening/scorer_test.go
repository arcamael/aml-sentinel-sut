// Golden-contract parity: the Go scorer must hit the same precision/recall
// targets on data/golden/matching.jsonl as the Python scorer (hard rule #2).
package main

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

type matchingRow struct {
	ProfileName   string `json:"profile_name"`
	CandidateName string `json:"candidate_name"`
	DOBProfile    string `json:"dob_profile"`
	DOBCandidate  string `json:"dob_candidate"`
	ExpectedMatch bool   `json:"expected_match"`
	MinScore      float64 `json:"min_score"`
}

func loadGolden(t *testing.T) []matchingRow {
	f, err := os.Open("../data/golden/matching.jsonl")
	if err != nil {
		t.Fatalf("open golden: %v", err)
	}
	defer f.Close()
	var rows []matchingRow
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r matchingRow
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("parse golden: %v", err)
		}
		rows = append(rows, r)
	}
	return rows
}

func TestGoldenPrecisionRecall(t *testing.T) {
	rows := loadGolden(t)
	var tp, fp, fn int
	for _, r := range rows {
		score := scorePair(r.ProfileName, r.CandidateName, r.DOBProfile, r.DOBCandidate)
		predicted := score >= ScreeningThreshold
		if r.ExpectedMatch && score < r.MinScore {
			t.Errorf("true pair below min_score: %q~%q score=%.4f min=%.4f",
				r.ProfileName, r.CandidateName, score, r.MinScore)
		}
		switch {
		case predicted && r.ExpectedMatch:
			tp++
		case predicted && !r.ExpectedMatch:
			fp++
		case !predicted && r.ExpectedMatch:
			fn++
		}
	}
	precision := 1.0
	if tp+fp > 0 {
		precision = float64(tp) / float64(tp+fp)
	}
	recall := 1.0
	if tp+fn > 0 {
		recall = float64(tp) / float64(tp+fn)
	}
	t.Logf("precision=%.3f recall=%.3f (tp=%d fp=%d fn=%d)", precision, recall, tp, fp, fn)
	if precision < 0.95 {
		t.Errorf("precision %.3f < 0.95", precision)
	}
	if recall < 0.90 {
		t.Errorf("recall %.3f < 0.90", recall)
	}
}
