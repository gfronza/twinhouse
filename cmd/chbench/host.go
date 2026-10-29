package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// quiescence waits for the benchmark's containers to go idle: after a load,
// twinhouse's stream worker re-applies views and ClickHouse merges parts in
// the background, and either would otherwise slow the reads timed next.
type quiescence struct {
	containers []string
	maxCPU     float64
	limit      time.Duration
	logf       func(string, ...any)
}

func (q *quiescence) wait(ctx context.Context) time.Duration {
	start := time.Now()
	calm := 0
	for calm < 3 && time.Since(start) < q.limit && ctx.Err() == nil {
		args := append([]string{"stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}"}, q.containers...)
		out, err := exec.CommandContext(ctx, "docker", args...).Output()
		if err != nil {
			q.logf("docker stats failed (%v); waiting 30s instead", err)
			time.Sleep(30 * time.Second)
			return time.Since(start)
		}
		busy := ""
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			name, pct, _ := strings.Cut(line, "\t")
			v, _ := strconv.ParseFloat(strings.TrimSuffix(pct, "%"), 64)
			if v > q.maxCPU {
				busy += fmt.Sprintf(" %s=%.0f%%", name, v)
			}
		}
		if busy == "" {
			calm++
		} else {
			calm = 0
			if int(time.Since(start).Seconds())%30 < 3 {
				q.logf("  still busy:%s", busy)
			}
		}
	}
	return time.Since(start)
}

// container is one container's image and resource caps.
type container struct {
	Name   string  `json:"name"`
	Image  string  `json:"image"`
	CPUs   float64 `json:"cpus,omitempty"`
	MemGiB float64 `json:"mem_gib,omitempty"`
}

func inspect(ctx context.Context, names []string) []container {
	args := append([]string{"inspect", "-f", "{{.Name}}\t{{.Config.Image}}\t{{.HostConfig.NanoCpus}}\t{{.HostConfig.Memory}}"}, names...)
	out, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil {
		return nil
	}
	var cs []container
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 4 {
			continue
		}
		nano, _ := strconv.ParseFloat(f[2], 64)
		mem, _ := strconv.ParseFloat(f[3], 64)
		cs = append(cs, container{Name: strings.TrimPrefix(f[0], "/"), Image: f[1], CPUs: nano / 1e9, MemGiB: mem / (1 << 30)})
	}
	return cs
}

func commit() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	c := strings.TrimSpace(string(out))
	if st, _ := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); len(st) > 0 {
		c += "+dirty"
	}
	return c
}

func loadAvg() string {
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		return strings.Join(strings.Fields(string(b))[:3], " ")
	}
	if out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output(); err == nil {
		return strings.Trim(strings.TrimSpace(string(out)), "{} ")
	}
	return "unknown"
}

func hostDesc() string {
	mem := ""
	if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
		if n, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64); err == nil {
			mem = fmt.Sprintf(", %.0f GiB", n/(1<<30))
		}
	}
	model := ""
	if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
		model = strings.TrimSpace(string(out)) + ", "
	}
	return fmt.Sprintf("%s%s/%s, %d CPUs%s", model, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), mem)
}
