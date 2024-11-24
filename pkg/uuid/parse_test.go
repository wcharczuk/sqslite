package uuid

import (
	"fmt"
	"testing"

	"go.charczuk.com/sdk/assert"
)

func Test_MustParse(t *testing.T) {
	validShort := V4().String()
	validParsedShort := MustParse(validShort)
	assert.ItsEqual(t, true, validParsedShort.IsV4())
	assert.ItsEqual(t, validShort, validParsedShort.String())

	var invalidParsed UUID
	var r interface{}
	func() {
		defer func() {
			r = recover()
		}()
		invalidParsed = MustParse("foobarbaz")
	}()
	assert.ItsEqual(t, false, r == nil)
	assert.ItsEqual(t, true, invalidParsed.IsZero())
}

func Test_Parse_v4_valid(t *testing.T) {
	validShort := V4().String()
	validParsedShort, err := Parse(validShort)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, true, validParsedShort.IsV4())
	assert.ItsEqual(t, validShort, validParsedShort.String())

	validFull := fmt.Sprintf("%+v", V4())
	validParsedFull, err := Parse(validFull)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, true, validParsedFull.IsV4())
	assert.ItsEqual(t, validFull, fmt.Sprintf("%+v", validParsedFull))

	validBracedShort := fmt.Sprintf("{%s}", validShort)
	validParsedBracedShort, err := Parse(validBracedShort)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, true, validParsedBracedShort.IsV4())
	assert.ItsEqual(t, validShort, validParsedBracedShort.String())

	validBracedFull := fmt.Sprintf("{%s}", validFull)
	validParsedBracedFull, err := Parse(validBracedFull)
	assert.ItsEqual(t, nil, err)
	assert.ItsEqual(t, true, validParsedBracedFull.IsV4())
	assert.ItsEqual(t, validFull, fmt.Sprintf("%+v", validParsedBracedFull))
}

func Test_Parse_v4_invalid(t *testing.T) {
	_, err := Parse("fcae3946f75d+3258678bb5e6795a6d3")
	assert.ItsNotNil(t, err, "should error for invalid characters")

	_, err = Parse("4f2e28b7b8f94b9eba1d90c4452")
	assert.ItsNotNil(t, err, "should error for invalid length uuids")
}

func Test_ParseInto_invalid(t *testing.T) {
	var uuid UUID
	var err error
	err = ParseInto(&uuid, "")
	assert.ItsEqual(t, nil, err)

	err = ParseInto(&uuid, "earth")
	assert.ItsEqual(t, ErrParseInvalidLength, err.Error())

	err = ParseInto(&uuid, "{aaa{aaa")
	assert.ItsEqual(t, ErrParseIllegalCharacter, err.Error())

	err = ParseInto(&uuid, "{aa}aa")
	assert.ItsEqual(t, ErrParseIllegalCharacter, err.Error())

	err = ParseInto(&uuid, "ab-cde")
	assert.ItsEqual(t, ErrParseIllegalCharacter, err.Error())

	err = ParseInto(&uuid, "88888888-4444-qqqq-4444-121212121212")
	assert.ItsEqual(t, ErrParseIllegalCharacter, err.Error())

	err = ParseInto(&uuid, "88888888-4444-4444-4444-88888888")
	assert.ItsEqual(t, ErrParseInvalidLength, err.Error())

	err = ParseInto(&uuid, "88888888-4444-4444-4444-1616161616161616")
	assert.ItsEqual(t, ErrParseInvalidLength, err.Error())
}

func Test_fromHexChar(t *testing.T) {

	for x := '0'; x <= '9'; x++ {
		c, ok := fromHexChar(byte(x))
		assert.ItsEqual(t, true, ok)
		assert.ItsEqual(t, x-'0', c)
	}

	for x := 'a'; x <= 'f'; x++ {
		c, ok := fromHexChar(byte(x))
		assert.ItsEqual(t, true, ok)
		assert.ItsEqual(t, x-'a'+10, c)
	}

	for x := 'A'; x <= 'F'; x++ {
		c, ok := fromHexChar(byte(x))
		assert.ItsEqual(t, true, ok)
		assert.ItsEqual(t, x-'A'+10, c)
	}

	c, ok := fromHexChar('z')
	assert.ItsEqual(t, false, ok)
	assert.ItsEqual(t, 0, c)
}
