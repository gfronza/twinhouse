//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"
)

// adEvent mirrors a row of ad_events.
type adEvent struct {
	Tenant   string
	Time     time.Time
	ID       string
	Campaign string
	User     string
	Type     string
	Cost     uint64
	Bot      uint8
	Country  *string
	ClientTS time.Time
}

var eventBase = time.Date(2024, 5, 6, 21, 0, 0, 0, time.UTC) // spans midnight → two days

// genEvents produces n deterministic events; prefix makes event IDs unique per batch.
func genEvents(seed int64, n int, prefix string) []adEvent {
	r := rand.New(rand.NewSource(seed))
	tenants := []string{"acme", "globex", "initech"}
	types := []string{"impression", "impression", "impression", "click", "conversion"}
	countries := []string{"US", "DE", "BR", "JP"}
	out := make([]adEvent, n)
	for i := range out {
		tn := tenants[r.Intn(len(tenants))]
		ts := eventBase.Add(time.Duration(r.Intn(6*3600)) * time.Second)
		e := adEvent{
			Tenant:   tn,
			Time:     ts,
			ID:       fmt.Sprintf("%s-%06d", prefix, i),
			Campaign: fmt.Sprintf("%s-cmp-%d", tn, r.Intn(4)),
			User:     fmt.Sprintf("u%03d", r.Intn(150)),
			Type:     types[r.Intn(len(types))],
			Cost:     uint64(r.Intn(5000)),
			ClientTS: ts.Add(time.Duration(r.Intn(1000)) * time.Millisecond),
		}
		if r.Intn(10) == 0 {
			e.Bot = 1
		}
		if r.Intn(4) != 0 {
			c := countries[r.Intn(len(countries))]
			e.Country = &c
		}
		out[i] = e
	}
	return out
}

// jsonEachRow renders events as NDJSON the way a Python loader would.
func jsonEachRow(evs []adEvent) []byte {
	var sb strings.Builder
	for _, e := range evs {
		m := map[string]any{
			"tenant_id":   e.Tenant,
			"event_time":  e.Time.Format("2006-01-02 15:04:05"),
			"event_id":    e.ID,
			"campaign_id": e.Campaign,
			"user_id":     e.User,
			"event_type":  e.Type,
			"cost_micros": e.Cost,
			"is_bot":      e.Bot,
			"country":     e.Country,
			"client_ts":   e.ClientTS.Format("2006-01-02 15:04:05.000"),
		}
		b, _ := json.Marshal(m)
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return []byte(sb.String())
}

type hourKey struct {
	Tenant, Campaign string
	Hour             time.Time
}

type hourAgg struct {
	Events, Impressions, Clicks, Cost uint64
	LastSeen                          time.Time
	Users                             map[string]bool
}

func (a hourAgg) String() string {
	return fmt.Sprintf("events=%d impressions=%d clicks=%d cost=%d last_seen=%s users=%d",
		a.Events, a.Impressions, a.Clicks, a.Cost, a.LastSeen.UTC().Format(time.DateTime), len(a.Users))
}

func expectedHourly(evs []adEvent) map[hourKey]*hourAgg {
	out := map[hourKey]*hourAgg{}
	for _, e := range evs {
		k := hourKey{e.Tenant, e.Campaign, e.Time.Truncate(time.Hour)}
		a := out[k]
		if a == nil {
			a = &hourAgg{Users: map[string]bool{}}
			out[k] = a
		}
		a.Events++
		if e.Type == "impression" {
			a.Impressions++
		}
		if e.Type == "click" {
			a.Clicks++
		}
		a.Cost += e.Cost
		if e.Time.After(a.LastSeen) {
			a.LastSeen = e.Time
		}
		a.Users[e.User] = true
	}
	return out
}

type dayKey struct {
	Tenant   string
	Day      string
	Campaign string
}

type dayAgg struct{ Clicks, Cost uint64 }

func expectedDaily(evs []adEvent) map[dayKey]dayAgg {
	out := map[dayKey]dayAgg{}
	for _, e := range evs {
		if e.Bot != 0 {
			continue
		}
		k := dayKey{e.Tenant, e.Time.Format("2006-01-02"), e.Campaign}
		a := out[k]
		if e.Type == "click" {
			a.Clicks++
		}
		a.Cost += e.Cost
		out[k] = a
	}
	return out
}

func sortedHourKeys(m map[hourKey]*hourAgg) []hourKey {
	ks := make([]hourKey, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		a, b := ks[i], ks[j]
		if a.Tenant != b.Tenant {
			return a.Tenant < b.Tenant
		}
		if a.Campaign != b.Campaign {
			return a.Campaign < b.Campaign
		}
		return a.Hour.Before(b.Hour)
	})
	return ks
}
