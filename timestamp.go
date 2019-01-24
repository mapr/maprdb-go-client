package private_maprdb_go_client

import (
	"encoding/json"
	"time"
)

type OTimestamp struct {
	dateTime time.Time
}

// Make OTimestamp from RFC3339 string representation
func MakeOTimestampFromString(str string) *OTimestamp {
	tm, err := time.Parse(time.RFC3339, str)
	if err != nil {
		panic(err)
	}
	return &OTimestamp{dateTime: tm}
}

// Make OTimestamp from time.Time
func MakeOTimestampFromDate(date time.Time) *OTimestamp {
	return &OTimestamp{dateTime: date}
}

// Make OTimestamp from Unix timestamp
func MakeOTimestampFromUnixTimestamp(sec, nsec int) *OTimestamp {
	//TODO fixme
	tm := time.Unix(int64(sec), int64(nsec))
	return &OTimestamp{dateTime: tm}
}

// Make OTimestamp from years, months, days, hours, minutes, seconds and millis
func MakeOTimestamp(years, months, days, hours, minutes, seconds, millis int) *OTimestamp {
	tm := time.Date(years, time.Month(months), days, hours, minutes, seconds, millis*1000000, time.UTC)
	return &OTimestamp{dateTime: tm}
}

// Returns time.Time of OTimestamp
func (timestamp *OTimestamp) DateTime() time.Time {
	return timestamp.dateTime
}

// Returns Years from OTimestamp
func (timestamp *OTimestamp) Years() int {
	return timestamp.dateTime.Year()
}

// Returns Months from OTimestamp
func (timestamp *OTimestamp) Months() int {
	return int(timestamp.dateTime.Month())
}

// Returns Days from OTimestamp
func (timestamp *OTimestamp) Days() int {
	return timestamp.dateTime.Day()
}

// Returns Hours from OTimestamp
func (timestamp *OTimestamp) Hours() int {
	return timestamp.dateTime.Hour()
}

// Returns Minutes from OTimestamp
func (timestamp *OTimestamp) Minutes() int {
	return timestamp.dateTime.Minute()
}

// Returns Millis from OTimestamp
func (timestamp *OTimestamp) Millis() int {
	return timestamp.dateTime.Nanosecond() / 1000000
}

// Stringer interface implementation
func (timestamp *OTimestamp) String() string {
	//TODO fixme
	return timestamp.dateTime.Format("2006-01-02T15:04:05.999999Z")
}

// Marshaller implementation for OTimestamp
func (timestamp *OTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(timestamp.String())
}
