// Package main — fuzzy scorer, a byte-for-byte port of the Python scorer
// (src/aml_sentinel/matching/fuzzy.py). Parity hinges on rapidfuzz's
// fuzz.ratio being the normalized Indel (LCS) similarity:
//
//	ratio(a, b) = 2 * LCS(a, b) / (len(a) + len(b)) * 100
//
// which is exactly reproducible. token_sort_ratio = ratio over space-joined,
// sorted tokens. name similarity = max(ratio, token_sort_ratio); DOB
// corroboration caps a mismatched DOB and lightly boosts a matching one.
package main

import (
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/mozillazg/go-unidecode"
)

const (
	ScreeningThreshold = 0.82
	DifferentDOBCap    = 0.75
	SameDOBBoost       = 0.03
)

var (
	nonNameChars = regexp.MustCompile(`[^\p{L}\p{N}\s'-]`)
	whitespace   = regexp.MustCompile(`\s+`)
)

// canonicalName mirrors aml_sentinel.matching.normalize.canonical_name:
// transliterate -> lowercase -> strip punctuation -> collapse whitespace ->
// trim stray hyphens/apostrophes at token edges.
func canonicalName(raw string) string {
	s := unidecode.Unidecode(raw)
	s = strings.ToLower(s)
	s = nonNameChars.ReplaceAllString(s, " ")
	s = whitespace.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	tokens := strings.Split(s, " ")
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		t = strings.Trim(t, "-'")
		if t != "" {
			out = append(out, t)
		}
	}
	return strings.Join(out, " ")
}

func lcs(a, b []rune) int {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			if a[i-1] == b[j-1] {
				curr[j] = prev[j-1] + 1
			} else if prev[j] >= curr[j-1] {
				curr[j] = prev[j]
			} else {
				curr[j] = curr[j-1]
			}
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

// ratio is rapidfuzz fuzz.ratio in [0,1] (Python divides by 100; we keep [0,1]).
func ratio(a, b string) float64 {
	ra, rb := []rune(a), []rune(b)
	total := len(ra) + len(rb)
	if total == 0 {
		return 1.0
	}
	return 2.0 * float64(lcs(ra, rb)) / float64(total)
}

func tokenSort(s string) string {
	tokens := strings.Fields(s)
	sort.Strings(tokens)
	return strings.Join(tokens, " ")
}

func tokenSortRatio(a, b string) float64 {
	return ratio(tokenSort(a), tokenSort(b))
}

func nameSimilarity(aCanonical, bCanonical string) float64 {
	return math.Max(ratio(aCanonical, bCanonical), tokenSortRatio(aCanonical, bCanonical))
}

func dobRelation(dobProfile, dobCandidate string) string {
	if dobProfile != "" && dobCandidate != "" {
		if dobProfile == dobCandidate {
			return "same"
		}
		return "different"
	}
	return "unknown"
}

func round4(x float64) float64 {
	return math.Round(x*10000) / 10000
}

// scorePair scores one profile-name vs one candidate-name with DOB corroboration.
func scorePair(profileName, candidateName, dobProfile, dobCandidate string) float64 {
	base := nameSimilarity(canonicalName(profileName), canonicalName(candidateName))
	switch dobRelation(dobProfile, dobCandidate) {
	case "different":
		return round4(math.Min(base, DifferentDOBCap))
	case "same":
		return round4(math.Min(1.0, base+SameDOBBoost))
	default:
		return round4(base)
	}
}

// ScoredCandidate is the best score for a candidate across entity_name + aliases.
type ScoredCandidate struct {
	Score       float64
	MatchedName string
	DOBMatch    bool
}

func scoreCandidate(profileName, profileDOB, entityName string, aliases []string, candidateDOB string) ScoredCandidate {
	best := -1.0
	bestName := entityName
	for _, name := range append([]string{entityName}, aliases...) {
		s := scorePair(profileName, name, profileDOB, candidateDOB)
		if s > best {
			best = s
			bestName = name
		}
	}
	return ScoredCandidate{Score: best, MatchedName: bestName, DOBMatch: dobRelation(profileDOB, candidateDOB) == "same"}
}
