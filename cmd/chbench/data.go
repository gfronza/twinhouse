package main

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"
)

// The dataset is generated from a seed, so both servers get the same rows
// and a rerun gets the same rows again.
const (
	tenants   = 100
	users     = 50_000
	versions  = 3
	spanDays  = 30
	batchRows = 10_000
)

var (
	dataStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	kinds     = []string{"view", "view", "view", "view", "click", "click", "search", "add_to_cart", "purchase", "signup"}
	plans     = []string{"free", "free", "free", "pro", "pro", "team"}
	countries = []string{"US", "BR", "DE", "IN", "GB", "FR", "JP", "CA"}
	pages     = []string{"products", "category", "search", "cart", "checkout", "account", "help", "blog"}
)

// eventGen yields the events in order. Timestamps strictly increase, so no
// two rows tie on ts and every ORDER BY ts result is deterministic.
type eventGen struct {
	r    *rand.Rand
	zipf *rand.Zipf
	n, i int
	step time.Duration
}

func newEventGen(seed uint64, n int) *eventGen {
	r := rand.New(rand.NewPCG(seed, 1))
	step := spanDays * 24 * time.Hour / time.Duration(n)
	return &eventGen{r: r, zipf: rand.NewZipf(r, 1.1, 1, tenants-1), n: n, step: step.Truncate(time.Millisecond)}
}

// batch returns the next up to max rows as JSONEachRow, and how many.
func (g *eventGen) batch(max int) ([]byte, int) {
	var b bytes.Buffer
	k := 0
	for ; k < max && g.i < g.n; k, g.i = k+1, g.i+1 {
		r := g.r
		ts := dataStart.Add(time.Duration(g.i) * g.step)
		kind := kinds[r.IntN(len(kinds))]
		amount := 0.0
		if kind == "purchase" {
			amount = float64(r.IntN(100_000)) / 100
		}
		var id [16]byte
		for j := range id {
			id[j] = byte(r.Uint32())
		}
		id[6] = id[6]&0x0f | 0x40
		id[8] = id[8]&0x3f | 0x80
		url := "https://shop.example.com/" + pages[r.IntN(len(pages))] + "/" + randPath(r, 5+r.IntN(80))
		fmt.Fprintf(&b, `{"tenant_id":%d,"ts":"%s","event_id":"%x-%x-%x-%x-%x","user_id":%d,"kind":"%s","url":"%s","duration_ms":%d,"amount":%s}`+"\n",
			g.zipf.Uint64()+1, ts.Format("2006-01-02 15:04:05.000"),
			id[0:4], id[4:6], id[6:8], id[8:10], id[10:16],
			r.IntN(users)+1, kind, url, r.IntN(5000)+1, strconv.FormatFloat(amount, 'f', -1, 64))
	}
	return b.Bytes(), k
}

func randPath(r *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-/"
	p := make([]byte, n)
	for i := range p {
		p[i] = alphabet[r.IntN(len(alphabet))]
	}
	return string(p)
}

// userBatches returns the users_latest load: every user once per version,
// version by version, so ClickHouse holds superseded rows in other parts.
func userBatches(seed uint64) [][]byte {
	r := rand.New(rand.NewPCG(seed, 2))
	var out [][]byte
	for v := 1; v <= versions; v++ {
		var b bytes.Buffer
		for u := 1; u <= users; u++ {
			at := dataStart.Add(time.Duration(v-1)*10*24*time.Hour + time.Duration(r.IntN(10*24*3600))*time.Second)
			fmt.Fprintf(&b, `{"user_id":%d,"ver":%d,"plan":"%s","country":"%s","updated_at":"%s"}`+"\n",
				u, v, plans[r.IntN(len(plans))], countries[r.IntN(len(countries))], at.Format(time.DateTime))
			if u%batchRows == 0 || u == users {
				out = append(out, bytes.Clone(b.Bytes()))
				b.Reset()
			}
		}
	}
	return out
}

// params draws query parameters from their own seeded stream, the same
// for both servers. Tenants follow the data's skew, so hot tenants are
// queried more often, as they would be in an application.
type params struct {
	r    *rand.Rand
	zipf *rand.Zipf
}

func newParams(seed uint64) *params {
	r := rand.New(rand.NewPCG(seed, 3))
	return &params{r: r, zipf: rand.NewZipf(r, 1.1, 1, tenants-1)}
}

func (p *params) tenant() uint64 { return p.zipf.Uint64() + 1 }
func (p *params) user() int      { return p.r.IntN(users) + 1 }
func (p *params) hour() time.Time {
	return dataStart.Add(time.Duration(p.r.IntN(spanDays*24)) * time.Hour)
}
func (p *params) minute() time.Time {
	return dataStart.Add(time.Duration(p.r.IntN(spanDays*24*60)) * time.Minute)
}
func (p *params) kind() string { return kinds[p.r.IntN(len(kinds))] }
