package uuid

import (
	"encoding/json"
	"fmt"
	"testing"

	"sqslite/pkg/assert"
)

func Test_UUID_Equal(t *testing.T) {
	uid0 := V4()
	uid1 := V4()

	assert.ItsEqual(t, true, uid0.Equal(uid0))
	assert.ItsEqual(t, false, uid0.Equal(uid1))
}

func Test_UUID_Compare(t *testing.T) {
	uid0 := V4()
	uid1 := V4()
	uid2 := V4()

	assert.ItsEqual(t, 0, uid1.Compare(uid1))
	assert.ItsEqual(t, true, uid1.Compare(uid0) != 0)
	assert.ItsEqual(t, true, uid1.Compare(uid2) != 0)
}

func Test_UUID_String(t *testing.T) {
	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)

	knownString := knownParsed.String()
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", knownString)
}

func Test_UUID_Format(t *testing.T) {
	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)

	vstr := fmt.Sprintf("%v", knownParsed)
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", vstr)

	vplusstr := fmt.Sprintf("%+v", knownParsed)
	assert.ItsEqual(t, knownUUID, vplusstr)

	str := fmt.Sprintf("%s", knownParsed)
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", str)

	versionstr := fmt.Sprintf("%q", knownParsed)
	assert.ItsEqual(t, "1", versionstr)
}

func Test_UUID_IsZero(t *testing.T) {
	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, false, knownParsed.IsZero())

	var zero UUID
	assert.ItsEqual(t, true, zero.IsZero())
}

func Test_UUID_IsV4(t *testing.T) {
	valid := makeTestUUID(0x40, 0x80)
	versionInvalid := makeTestUUID(0xF0, 0x80)
	variantInvalid := makeTestUUID(0x40, 0xF0)

	assert.ItsEqual(t, true, valid.IsV4())
	assert.ItsEqual(t, false, variantInvalid.IsV4())
	assert.ItsEqual(t, false, versionInvalid.IsV4())
}

type marshalTest struct {
	ID UUID `json:"id" yaml:"id"`
}

func Test_UUID_MarshalJSON(t *testing.T) {
	id := V4()
	rawJSON := []byte(fmt.Sprintf(`{"id":"%s"}`, id.String()))

	var testVal marshalTest
	assert.ItsEqual(t, nil, json.Unmarshal(rawJSON, &testVal))
	assert.ItsEqual(t, id.String(), testVal.ID.String())

	newJSON, err := json.Marshal(testVal)
	assert.ItsEqual(t, nil, err)

	var verify marshalTest
	assert.ItsEqual(t, nil, json.Unmarshal(newJSON, &verify))
	assert.ItsEqual(t, id.String(), verify.ID.String())
}

func Test_UUID_MarshalJSON_format(t *testing.T) {
	id := V4()

	marshaled, err := id.MarshalJSON()
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, fmt.Sprintf("%q", id.String()), string(marshaled))
}

func Test_UUID_Marshal(t *testing.T) {
	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)

	b, err := knownParsed.Marshal()
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, 16, len(b))
}

func Test_UUID_MarshalTo(t *testing.T) {
	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)

	buffer := make([]byte, 16)
	n, err := knownParsed.MarshalTo(buffer)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, 16, n)
	assert.ItsEqual(t, buffer, []byte(knownParsed[:]))
}

func Test_UUID_Unmarshal(t *testing.T) {
	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)

	b, err := knownParsed.Marshal()
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, 16, len(b))

	roundTrip := new(UUID)
	err = roundTrip.Unmarshal(b)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, true, roundTrip.Equal(knownParsed))

	err = roundTrip.Unmarshal(nil)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, true, roundTrip.Equal(knownParsed))
}

func Test_UUID_Size(t *testing.T) {
	var zero *UUID
	assert.ItsEqual(t, 0, zero.Size())

	var notZero UUID
	assert.ItsEqual(t, 16, (&notZero).Size())
}

func Test_UUID_MarshalYAML(t *testing.T) {
	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)

	b, err := knownParsed.MarshalYAML()
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", b)
}

func Test_UUID_UnmarshalYAML(t *testing.T) {
	var uuid UUID
	err := (&uuid).UnmarshalYAML(func(v interface{}) error {
		return fmt.Errorf("this is just a test")
	})
	assert.ItsEqual(t, "this is just a test", err.Error())

	err = (&uuid).UnmarshalYAML(func(v interface{}) error {
		typed, ok := v.(*string)
		if !ok {
			return fmt.Errorf("provided dst is not a *string")
		}
		*typed = "ebc3e852-86c8-11ec-9ddd-00155de08255"
		return nil
	})
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", uuid.String())
}

func Test_UUID_Scan_string(t *testing.T) {
	uid := UUID{}

	err := (&uid).Scan("ebc3e852-86c8-11ec-9ddd-00155de08255")
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", uid.String())
}

func Test_UUID_Scan_bytes(t *testing.T) {
	uid := UUID{}

	err := (&uid).Scan([]byte("ebc3e852-86c8-11ec-9ddd-00155de08255"))
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", uid.String())
}

func Test_UUID_Scan_invalid(t *testing.T) {
	uid := UUID{}
	err := (&uid).Scan(3.14)
	assert.ItsEqual(t, ErrInvalidScanSource, err.Error())
}

func Test_UUID_Value(t *testing.T) {
	var zero UUID
	value, err := zero.Value()
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, nil, value)

	knownUUID := "ebc3e852-86c8-11ec-9ddd-00155de08255"
	knownParsed, err := Parse(knownUUID)
	assert.ItsEqual(t, nil, err)

	value, err = knownParsed.Value()
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, "ebc3e85286c811ec9ddd00155de08255", value)
}
