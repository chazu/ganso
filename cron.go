package honker

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Schedule represents a recurring time schedule.
type Schedule interface {
	NextAfter(t time.Time) time.Time
	Expr() string // returns the original expression
}

// CronSchedule is a parsed cron expression.
type CronSchedule struct {
	expr    string
	seconds []int
	minutes []int
	hours   []int
	days    []int
	months  []int
	dows    []int // 0=Sun..6=Sat
}

func (c *CronSchedule) Expr() string { return c.expr }

// IntervalSchedule fires at fixed intervals.
type IntervalSchedule struct {
	interval time.Duration
	expr     string
}

func (s *IntervalSchedule) Expr() string              { return s.expr }
func (s *IntervalSchedule) NextAfter(t time.Time) time.Time { return t.Add(s.interval) }

// ParseSchedule parses a cron expression or interval expression.
func ParseSchedule(expr string) (Schedule, error) {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "@every ") {
		return parseInterval(expr)
	}
	return parseCron(expr)
}

// Crontab parses a 5 or 6 field cron expression only (not intervals).
func Crontab(expr string) (Schedule, error) {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "@every ") {
		return nil, fmt.Errorf("honker: Crontab does not accept interval expressions: %q", expr)
	}
	return parseCron(expr)
}

// Every returns an interval schedule.
func Every(d time.Duration) Schedule {
	secs := int(d.Seconds())
	return &IntervalSchedule{interval: d, expr: fmt.Sprintf("@every %ds", secs)}
}

// parseInterval parses "@every <n><unit>" where unit is s, m, h, or d.
func parseInterval(expr string) (Schedule, error) {
	raw := strings.TrimPrefix(expr, "@every ")
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 {
		return nil, fmt.Errorf("honker: invalid interval expression: %q", expr)
	}

	unit := raw[len(raw)-1]
	digits := raw[:len(raw)-1]
	n, err := strconv.Atoi(digits)
	if err != nil {
		return nil, fmt.Errorf("honker: invalid interval number in %q: %w", expr, err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("honker: interval must be positive in %q", expr)
	}

	var d time.Duration
	switch unit {
	case 's':
		d = time.Duration(n) * time.Second
	case 'm':
		d = time.Duration(n) * time.Minute
	case 'h':
		d = time.Duration(n) * time.Hour
	case 'd':
		d = time.Duration(n) * 24 * time.Hour
	default:
		return nil, fmt.Errorf("honker: unknown interval unit %q in %q", string(unit), expr)
	}

	return &IntervalSchedule{interval: d, expr: expr}, nil
}

// parseCron parses a 5-field or 6-field cron expression.
func parseCron(expr string) (*CronSchedule, error) {
	fields := strings.Fields(expr)
	cs := &CronSchedule{expr: expr}

	var err error
	switch len(fields) {
	case 5:
		// minute hour dom month dow
		cs.seconds = []int{0}
		if cs.minutes, err = parseField(fields[0], 0, 59); err != nil {
			return nil, fmt.Errorf("honker: minute field: %w", err)
		}
		if cs.hours, err = parseField(fields[1], 0, 23); err != nil {
			return nil, fmt.Errorf("honker: hour field: %w", err)
		}
		if cs.days, err = parseField(fields[2], 1, 31); err != nil {
			return nil, fmt.Errorf("honker: day field: %w", err)
		}
		if cs.months, err = parseField(fields[3], 1, 12); err != nil {
			return nil, fmt.Errorf("honker: month field: %w", err)
		}
		if cs.dows, err = parseField(fields[4], 0, 6); err != nil {
			return nil, fmt.Errorf("honker: dow field: %w", err)
		}
	case 6:
		// second minute hour dom month dow
		if cs.seconds, err = parseField(fields[0], 0, 59); err != nil {
			return nil, fmt.Errorf("honker: second field: %w", err)
		}
		if cs.minutes, err = parseField(fields[1], 0, 59); err != nil {
			return nil, fmt.Errorf("honker: minute field: %w", err)
		}
		if cs.hours, err = parseField(fields[2], 0, 23); err != nil {
			return nil, fmt.Errorf("honker: hour field: %w", err)
		}
		if cs.days, err = parseField(fields[3], 1, 31); err != nil {
			return nil, fmt.Errorf("honker: day field: %w", err)
		}
		if cs.months, err = parseField(fields[4], 1, 12); err != nil {
			return nil, fmt.Errorf("honker: month field: %w", err)
		}
		if cs.dows, err = parseField(fields[5], 0, 6); err != nil {
			return nil, fmt.Errorf("honker: dow field: %w", err)
		}
	default:
		return nil, fmt.Errorf("honker: expected 5 or 6 cron fields, got %d in %q", len(fields), expr)
	}
	return cs, nil
}

// parseField parses a single cron field (e.g. "*/5", "1-10/2", "1,3,5", "*").
func parseField(field string, lo, hi int) ([]int, error) {
	seen := make(map[int]bool)
	parts := strings.Split(field, ",")
	for _, part := range parts {
		stepParts := strings.SplitN(part, "/", 2)
		rangePart := stepParts[0]
		step := 1
		if len(stepParts) == 2 {
			var err error
			step, err = strconv.Atoi(stepParts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid step %q: %w", stepParts[1], err)
			}
			if step <= 0 {
				return nil, fmt.Errorf("step must be positive, got %d", step)
			}
		}

		var start, end int
		if rangePart == "*" {
			start = lo
			end = hi
		} else if idx := strings.Index(rangePart, "-"); idx >= 0 {
			var err error
			start, err = strconv.Atoi(rangePart[:idx])
			if err != nil {
				return nil, fmt.Errorf("invalid range start %q: %w", rangePart[:idx], err)
			}
			end, err = strconv.Atoi(rangePart[idx+1:])
			if err != nil {
				return nil, fmt.Errorf("invalid range end %q: %w", rangePart[idx+1:], err)
			}
		} else {
			v, err := strconv.Atoi(rangePart)
			if err != nil {
				return nil, fmt.Errorf("invalid value %q: %w", rangePart, err)
			}
			start = v
			end = v
		}

		if start < lo || end > hi || start > end {
			return nil, fmt.Errorf("value out of range [%d-%d]: %d-%d", lo, hi, start, end)
		}

		for v := start; v <= end; v += step {
			seen[v] = true
		}
	}

	result := make([]int, 0, len(seen))
	for v := range seen {
		result = append(result, v)
	}
	sort.Ints(result)
	return result, nil
}

// nextOrFirst finds the first element in set >= current.
// Returns (value, wrapped). If no element >= current, wraps to set[0].
func nextOrFirst(set []int, current int) (int, bool) {
	for _, v := range set {
		if v >= current {
			return v, false
		}
	}
	return set[0], true
}

// contains checks if v is in the sorted set.
func contains(set []int, v int) bool {
	i := sort.SearchInts(set, v)
	return i < len(set) && set[i] == v
}

// NextAfter returns the next time the cron schedule fires strictly after t.
func (c *CronSchedule) NextAfter(from time.Time) time.Time {
	// Start one second after from, truncated to the second.
	candidate := from.Add(time.Second).Truncate(time.Second)
	maxYear := from.Year() + 100

	for candidate.Year() <= maxYear {
		// 1. Month
		if !contains(c.months, int(candidate.Month())) {
			m, wrapped := nextOrFirst(c.months, int(candidate.Month()))
			y := candidate.Year()
			if wrapped {
				y++
			}
			candidate = time.Date(y, time.Month(m), 1, 0, 0, 0, 0, candidate.Location())
			continue
		}

		// 2. Day-of-month and day-of-week
		if !contains(c.days, candidate.Day()) || !contains(c.dows, int(candidate.Weekday())) {
			candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day()+1,
				0, 0, 0, 0, candidate.Location())
			continue
		}

		// 3. Hour
		if !contains(c.hours, candidate.Hour()) {
			h, wrapped := nextOrFirst(c.hours, candidate.Hour())
			if wrapped {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day()+1,
					0, 0, 0, 0, candidate.Location())
			} else {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(),
					h, 0, 0, 0, candidate.Location())
			}
			continue
		}

		// 4. Minute
		if !contains(c.minutes, candidate.Minute()) {
			m, wrapped := nextOrFirst(c.minutes, candidate.Minute())
			if wrapped {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(),
					candidate.Hour()+1, 0, 0, 0, candidate.Location())
			} else {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(),
					candidate.Hour(), m, 0, 0, candidate.Location())
			}
			continue
		}

		// 5. Second
		if !contains(c.seconds, candidate.Second()) {
			s, wrapped := nextOrFirst(c.seconds, candidate.Second())
			if wrapped {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(),
					candidate.Hour(), candidate.Minute()+1, 0, 0, candidate.Location())
			} else {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(),
					candidate.Hour(), candidate.Minute(), s, 0, candidate.Location())
			}
			continue
		}

		// All fields match.
		return candidate
	}

	// Should never happen with reasonable inputs.
	return time.Time{}
}
