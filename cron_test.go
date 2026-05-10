package ganso

import (
	"testing"
	"time"
)

// April 19, 2026 is a Sunday.
var testBase = time.Date(2026, 4, 19, 0, 0, 0, 0, time.Local)

func TestCronEveryMinute(t *testing.T) {
	s, err := ParseSchedule("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 4, 19, 12, 30, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 19, 12, 31, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCronEvery5Minutes(t *testing.T) {
	s, err := ParseSchedule("*/5 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 4, 19, 12, 30, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 19, 12, 35, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCron6FieldEvery10Seconds(t *testing.T) {
	s, err := ParseSchedule("*/10 * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 4, 19, 12, 30, 5, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 19, 12, 30, 10, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestIntervalEvery1Second(t *testing.T) {
	s, err := ParseSchedule("@every 1s")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 4, 19, 12, 30, 5, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 19, 12, 30, 6, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCron3AM(t *testing.T) {
	s, err := ParseSchedule("0 3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	// From 10:00, next 3AM is next day.
	from := time.Date(2026, 4, 19, 10, 0, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 20, 3, 0, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCron3AMFromExact(t *testing.T) {
	s, err := ParseSchedule("0 3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	// From exactly 03:00:00, must be strictly after -> next day 03:00:00.
	from := time.Date(2026, 4, 19, 3, 0, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 20, 3, 0, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCronRangeWithStep(t *testing.T) {
	s, err := ParseSchedule("0-30/10 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 4, 19, 12, 5, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 19, 12, 10, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCronList(t *testing.T) {
	s, err := ParseSchedule("0,30 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 4, 19, 12, 10, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 19, 12, 30, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCronDOWMonday(t *testing.T) {
	// "0 12 * * 1" = noon on Mondays
	s, err := ParseSchedule("0 12 * * 1")
	if err != nil {
		t.Fatal(err)
	}
	// April 19 2026 is Sunday. Next Monday is April 20.
	from := time.Date(2026, 4, 19, 0, 0, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 20, 12, 0, 0, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCronErrorTooFewFields(t *testing.T) {
	_, err := ParseSchedule("* * * *")
	if err == nil {
		t.Error("expected error for 4-field cron")
	}
}

func TestIntervalErrorZero(t *testing.T) {
	_, err := ParseSchedule("@every 0s")
	if err == nil {
		t.Error("expected error for zero interval")
	}
}

func TestIntervalErrorUnknownUnit(t *testing.T) {
	_, err := ParseSchedule("@every 5w")
	if err == nil {
		t.Error("expected error for unknown unit")
	}
}

func TestParseScheduleCron(t *testing.T) {
	s, err := ParseSchedule("*/15 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.(*CronSchedule); !ok {
		t.Errorf("expected *CronSchedule, got %T", s)
	}
	if s.Expr() != "*/15 * * * *" {
		t.Errorf("Expr() = %q, want %q", s.Expr(), "*/15 * * * *")
	}
}

func TestParseScheduleInterval(t *testing.T) {
	s, err := ParseSchedule("@every 30m")
	if err != nil {
		t.Fatal(err)
	}
	is, ok := s.(*IntervalSchedule)
	if !ok {
		t.Fatalf("expected *IntervalSchedule, got %T", s)
	}
	if is.interval != 30*time.Minute {
		t.Errorf("interval = %v, want %v", is.interval, 30*time.Minute)
	}
}

func TestCrontabRejectsInterval(t *testing.T) {
	_, err := Crontab("@every 5s")
	if err == nil {
		t.Error("expected Crontab to reject @every expressions")
	}
}

func TestEveryHelper(t *testing.T) {
	s := Every(10 * time.Second)
	from := time.Date(2026, 4, 19, 12, 0, 0, 0, time.Local)
	got := s.NextAfter(from)
	want := time.Date(2026, 4, 19, 12, 0, 10, 0, time.Local)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
