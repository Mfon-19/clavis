package main

import (
	"fmt"
	"os"
	"slices"
	"text/tabwriter"
	"time"
)

// samples is a set of latency measurements.
type samples []time.Duration

// pct returns the p-th percentile (0-100) using nearest rank.
func (s samples) pct(p float64) time.Duration {
	if len(s) == 0 {
		return 0
	}
	sorted := slices.Clone(s)
	slices.Sort(sorted)
	rank := int(p/100*float64(len(sorted))+0.5) - 1
	return sorted[max(0, min(rank, len(sorted)-1))]
}

func (s samples) max() time.Duration {
	if len(s) == 0 {
		return 0
	}
	return slices.Max(s)
}

func (s samples) min() time.Duration {
	if len(s) == 0 {
		return 0
	}
	return slices.Min(s)
}

// ms formats a duration in milliseconds with precision suited to its size.
func ms(d time.Duration) string {
	v := float64(d) / float64(time.Millisecond)
	switch {
	case v < 10:
		return fmt.Sprintf("%.2fms", v)
	case v < 1000:
		return fmt.Sprintf("%.1fms", v)
	default:
		return fmt.Sprintf("%.2fs", v/1000)
	}
}

// table prints aligned rows; the first row is the header.
func table(rows ...[]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	for _, row := range rows {
		for i, cell := range row {
			if i > 0 {
				fmt.Fprint(w, "\t")
			}
			fmt.Fprint(w, cell)
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}
