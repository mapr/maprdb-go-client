package private_maprdb_go_client

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// OJAI time representation
type OTime struct {
	t time.Time
}

// Make OTime from Unix timestamp in int64 format
func MakeOTimeFromTimestampInt(timestamp int64) *OTime {
	return &OTime{t: time.Unix(timestamp, 0)}
}

// Make OTime from Unix timestamp in string format
func MakeOTimeFromTimestampString(timestamp string) (*OTime, error) {
	i, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return nil, err
	}
	return &OTime{t: time.Unix(i, 0)}, nil
}

// Make OTime from string in RFC3339 format.
// example : "2015-11-11T10:30:24.354Z"
func MakeOTimeFromStringRFC3339(date string) (*OTime, error) {
	t, err := time.Parse(time.RFC3339, date)
	if err != nil {
		return nil, err
	}
	return &OTime{t: t}, nil
}

// default layout
var layout = "15:04:05"

// Make OTime from string and layout format
func MakeOTimeFromString(date string) (*OTime, error) {
	t, err := time.Parse(layout, date)
	if err != nil {
		return nil, err
	}
	return &OTime{t: t}, nil
}

func MakeOTimeFromDate(date *time.Time) *OTime {
	return &OTime{t: *date}
}

func MakeOTime(hourOfDay, minutes, seconds int) *OTime {
	return &OTime{t: time.Date(0, 1, 0, hourOfDay, minutes, seconds, 0, time.Local)}
}

func MakeOTimeWithNsec(hourOfDay, minutes, seconds, nsec int) *OTime {
	return &OTime{t: time.Date(0, 1, 0, hourOfDay, minutes, seconds, nsec, time.Local)}
}

// GetHour returns time hour of the OTime
func (time *OTime) GetHour() int {
	return time.t.Hour()
}

// GetMinute returns time minute of the OTime
func (time *OTime) GetMinute() int {
	return time.t.Minute()
}

// GetSecond returns second of the OTime
func (time *OTime) GetSecond() int {
	return time.t.Second()
}

// GetNanosecond returns nanosecond of the OTime
func (time *OTime) GetNanosecond() int {
	return time.t.Nanosecond()
}

// GetTime returns time.Time of the OTime
func (time *OTime) GetTime() *time.Time {
	return &time.t
}

// GetTimeString returns time as string
func (time *OTime) GetTimeString() string {
	return time.String()
}

// Stringer interface implementation
func (time *OTime) String() string {
	return fmt.Sprintf("%v:%v:%v", time.t.Hour(), time.t.Minute(), time.t.Second())
}

// Marshaller implementation for OTime
func (time *OTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.String())
}
