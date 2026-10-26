//go:build compat

package compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// statisticsRe matches the "statistics" object of JSON/JSONCompact output
// (elapsed, rows_read, bytes_read legitimately differ between servers).
var statisticsRe = regexp.MustCompile(`(?s),\n\n\t"statistics":\n\t\{.*?\n\t\}`)

func isJSONDoc(format string) bool {
	switch format {
	case "JSON", "JSONCompact", "JSONStrings", "JSONCompactStrings", "JSONColumns", "JSONColumnsWithMetadata", "JSONCompactColumns":
		return true
	}
	return false
}

// headerLines is the number of leading non-data lines of a text format.
func headerLines(format string) int {
	switch format {
	case "TSVWithNames", "TabSeparatedWithNames", "CSVWithNames", "TSVRawWithNames", "TabSeparatedRawWithNames",
		"CustomSeparatedWithNames", "JSONCompactEachRowWithNames":
		return 1
	case "TSVWithNamesAndTypes", "TabSeparatedWithNamesAndTypes", "CSVWithNamesAndTypes", "TSVRawWithNamesAndTypes",
		"TabSeparatedRawWithNamesAndTypes", "JSONCompactEachRowWithNamesAndTypes":
		return 2
	}
	return 0
}

// normalizeBody prepares a response body for comparison.
func normalizeBody(format string, body []byte, unordered bool) string {
	if isJSONDoc(format) {
		body = statisticsRe.ReplaceAll(body, nil)
		if unordered {
			return canonicalJSONDoc(body)
		}
		return string(body)
	}
	if !unordered {
		return string(body)
	}
	s := string(body)
	trailingNL := strings.HasSuffix(s, "\n")
	lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	h := min(headerLines(format), len(lines))
	data := append([]string(nil), lines[h:]...)
	sort.Strings(data)
	out := strings.Join(append(lines[:h:h], data...), "\n")
	if trailingNL {
		out += "\n"
	}
	return out
}

// canonicalJSONDoc re-encodes a JSON document with its "data" rows sorted.
func canonicalJSONDoc(body []byte) string {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return string(body)
	}
	delete(doc, "statistics")
	if data, ok := doc["data"].([]any); ok {
		keys := make([]string, len(data))
		idx := make([]int, len(data))
		for i, r := range data {
			b, _ := json.Marshal(r)
			keys[i] = string(b)
			idx[i] = i
		}
		sort.Slice(idx, func(a, b int) bool { return keys[idx[a]] < keys[idx[b]] })
		sorted := make([]any, len(data))
		for i, j := range idx {
			sorted[i] = data[j]
		}
		doc["data"] = sorted
	}
	b, _ := json.MarshalIndent(doc, "", "  ")
	return string(b)
}

// unifiedDiff renders a line diff of want (reference) vs got (twinhouse).
func unifiedDiff(want, got string) string {
	a := strings.Split(want, "\n")
	b := strings.Split(got, "\n")
	const maxLines = 3000
	if len(a) > maxLines || len(b) > maxLines {
		return fmt.Sprintf("--- reference (%d lines)\n+++ twinhouse (%d lines)\n(too large to diff; first lines)\n- %s\n+ %s\n",
			len(a), len(b), truncate(want, 2000), truncate(got, 2000))
	}
	// LCS table.
	n, m := len(a), len(b)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	type op struct {
		c    byte
		line string
	}
	var ops []op
	i, j := 0, 0
	for i < n || j < m {
		switch {
		case i < n && j < m && a[i] == b[j]:
			ops = append(ops, op{' ', a[i]})
			i++
			j++
		case i < n && (j == m || lcs[i+1][j] >= lcs[i][j+1]):
			ops = append(ops, op{'-', a[i]})
			i++
		default:
			ops = append(ops, op{'+', b[j]})
			j++
		}
	}
	var sb strings.Builder
	sb.WriteString("--- reference\n+++ twinhouse\n")
	const ctx = 3
	keep := make([]bool, len(ops))
	for k, o := range ops {
		if o.c != ' ' {
			for d := max(0, k-ctx); d <= min(len(ops)-1, k+ctx); d++ {
				keep[d] = true
			}
		}
	}
	gap := false
	for k, o := range ops {
		if !keep[k] {
			gap = true
			continue
		}
		if gap {
			sb.WriteString("@@\n")
			gap = false
		}
		sb.WriteByte(o.c)
		sb.WriteString(visible(o.line))
		sb.WriteByte('\n')
	}
	return sb.String()
}

// visible makes tabs and carriage returns visible in diffs.
func visible(s string) string {
	s = strings.ReplaceAll(s, "\t", "→")
	s = strings.ReplaceAll(s, "\r", "␍")
	return s
}
