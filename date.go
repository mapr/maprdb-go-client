package private_maprdb_go_client

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

const RFC3339FullDate = "2006-01-02"

// OJAI Date day representation
type ODate struct {
	d time.Time
}

// MakeODateFromTimestampInt creates and returns date from Unix timestamp in int64 format
func MakeODateFromTimestampInt(timestamp int64) *ODate {
	return &ODate{d: time.Unix(timestamp, 0)}
}

// MakeODateFromTimestampString creates and returns date from Unix timestamp in string format
func MakeODateFromTimestampString(timestamp string) *ODate {
	i, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		panic(err)
	}
	return &ODate{d: time.Unix(i, 0)}
}

// MakeODateFromString creates and returns date from string in RFC3339FullDate format.
// example : "2015-11-11"
func MakeODateFromString(date string) (*ODate, error) {
	t, err := time.Parse(RFC3339FullDate, date)
	if err != nil {
		return nil, err
	}
	return &ODate{d: t}, nil
}

// MakeODateFromDate creates and returns ODate from time.Time.
func MakeODateFromDate(date *time.Time) *ODate {
	return &ODate{d: *date}
}

// MakeDate creates and returns ODate from year, month and day of month.
func MakeODate(year, month, dayOfMonth int) *ODate {
	return &ODate{d: time.Date(year, time.Month(month), dayOfMonth, 0, 0, 0, 0, time.UTC)}
}

// GetYear returns year of the ODate
func (date *ODate) GetYear() int {
	return date.d.Year()
}

// GetMonth returns month of the ODate
func (date *ODate) GetMonth() int {
	return int(date.d.Month())
}

// GetDay returns day of the ODate
func (date *ODate) GetDay() int {
	return date.d.Day()
}

// GetDate returns time.Time of the ODate
func (date *ODate) GetDate() time.Time {
	return date.d
}

// Stringer interface implementation
func (date *ODate) String() string {
	return fmt.Sprintf("%v-%v-%v", date.d.Year(), int(date.d.Month()), date.d.Day())
}

// Marshaller implementation for ODate
func (date *ODate) MarshalJSON() ([]byte, error) {
	return json.Marshal(date.String())
}
