package uuid

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidScanSource is an error returned by scan.
const (
	ErrInvalidScanSource = "uuid; invalid scan source"
)

var (
	_ sql.Scanner = (*UUID)(nil)
)

// UUID represents a unique identifier conforming to the RFC 4122 standard.
// UUIDs are a fixed 128bit (16 byte) binary blob.
type UUID [16]byte

// Equal returns if a uuid is equal to another uuid.
func (uuid UUID) Equal(other UUID) bool {
	return bytes.Equal(uuid[0:], other[0:])
}

// Compare returns a comparison between the two uuids.
func (uuid UUID) Compare(other UUID) int {
	return bytes.Compare(uuid[0:], other[0:])
}

// String returns the uuid as a hex string.
func (uuid UUID) String() string {
	return hex.EncodeToString([]byte(uuid[:]))
}

// ShortString returns the first 8 bytes of the uuid as a hex string.
func (uuid UUID) ShortString() string {
	return hex.EncodeToString([]byte(uuid[:8]))
}

// Version returns the version byte of a uuid.
func (uuid UUID) Version() byte {
	return uuid[6] >> 4
}

// Format allows for conditional expansion in printf statements
// based on the token and flags used.
func (uuid UUID) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			b := []byte(uuid[:])
			fmt.Fprintf(s,
				"%08x-%04x-%04x-%04x-%012x",
				b[:4], b[4:6], b[6:8], b[8:10], b[10:],
			)
			return
		}
		fmt.Fprint(s, hex.EncodeToString([]byte(uuid[:])))
	case 's':
		fmt.Fprint(s, hex.EncodeToString([]byte(uuid[:])))
	case 'q':
		fmt.Fprintf(s, "%b", uuid.Version())
	}
}

// IsZero returns if the uuid is unset.
func (uuid UUID) IsZero() bool {
	return uuid == [16]byte{}
}

// IsV4 returns true iff uuid has version number 4, variant number 2, and length 16 bytes
func (uuid UUID) IsV4() bool {
	// check that version number is 4
	if (uuid[6]&0xf0)^0x40 != 0 {
		return false
	}
	// check that variant is 2
	return (uuid[8]&0xc0)^0x80 == 0
}

// Marshal implements bytes marshal.
func (uuid UUID) Marshal() ([]byte, error) {
	return []byte(uuid[:]), nil
}

// MarshalTo marshals the uuid to a buffer.
func (uuid UUID) MarshalTo(data []byte) (n int, err error) {
	copy(data, uuid[:])
	return 16, nil
}

// Unmarshal implements bytes unmarshal.
func (uuid *UUID) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var id UUID
	copy(id[:], data)
	*uuid = id
	return nil
}

// Size returns the size of the uuid.
func (uuid *UUID) Size() int {
	if uuid == nil {
		return 0
	}
	return 16
}

// MarshalJSON marshals a uuid as json.
func (uuid UUID) MarshalJSON() ([]byte, error) {
	return json.Marshal(uuid.String())
}

// UnmarshalJSON unmarshals a uuid from json.
func (uuid *UUID) UnmarshalJSON(corpus []byte) error {
	raw := strings.TrimSpace(string(corpus))
	raw = strings.TrimPrefix(raw, "\"")
	raw = strings.TrimSuffix(raw, "\"")
	return ParseInto(uuid, raw)
}

// MarshalYAML marshals a uuid as yaml.
func (uuid UUID) MarshalYAML() (interface{}, error) {
	return uuid.String(), nil
}

// UnmarshalYAML unmarshals a uuid from yaml.
func (uuid *UUID) UnmarshalYAML(unmarshaler func(interface{}) error) error {
	var corpus string
	if err := unmarshaler(&corpus); err != nil {
		return err
	}

	raw := strings.TrimSpace(string(corpus))
	raw = strings.TrimPrefix(raw, "\"")
	raw = strings.TrimSuffix(raw, "\"")
	return ParseInto(uuid, raw)
}

// Scan scans a uuid from a db value.
func (uuid *UUID) Scan(src interface{}) error {
	switch v := src.(type) {
	case string:
		return ParseInto(uuid, v)
	case []byte:
		return ParseInto(uuid, string(v))
	default:
		return errors.New(ErrInvalidScanSource)
	}
}

// Value returns a sql driver value.
func (uuid UUID) Value() (driver.Value, error) {
	if uuid.IsZero() {
		return nil, nil
	}
	return uuid.String(), nil
}

// Set accepts a src and populates the value based on it.
//
// This is similar to Scan but used directly by pgx.
func (uuid *UUID) Set(src interface{}) error {
	if src == nil {
		*uuid = [16]byte{}
		return nil
	}

	switch value := src.(type) {
	case interface{ Get() interface{} }:
		value2 := value.Get()
		if value2 != value {
			return uuid.Set(value2)
		}
	case fmt.Stringer:
		value2 := value.String()
		return uuid.Set(value2)
	case [16]byte:
		*uuid = value
	case []byte:
		if value != nil {
			if len(value) != 16 {
				return fmt.Errorf("[]byte must be 16 bytes to convert to UUID: %d", len(value))
			}
			copy(uuid[:], value)
		} else {
			*uuid = [16]byte{}
		}
	case string:
		return ParseInto(uuid, value)
	case *string:
		return ParseInto(uuid, *value)
	default:
		return fmt.Errorf("cannot convert %v to UUID", value)
	}
	return nil
}
