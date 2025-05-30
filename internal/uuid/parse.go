package uuid

import "errors"

// Error Classes
const (
	ErrParseInvalidUUIDInput = "parse uuid: existing uuid is invalid"
	ErrParseInvalidLength    = "parse uuid: input is an invalid length"
	ErrParseIllegalCharacter = "parse uuid: illegal character"
)

// MustParse parses a uuid and will panic if there is an error.
func MustParse(corpus string) (uuid UUID) {
	if err := ParseInto(&uuid, corpus); err != nil {
		panic(err)
	}
	return
}

// Parse parses a uuidv4 from a given string.
// valid forms are:
// - {xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}
// - xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
// - xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
func Parse(corpus string) (uuid UUID, err error) {
	err = ParseInto(&uuid, corpus)
	return
}

// ParseInto parses into an existing UUID.
func ParseInto(uuid *UUID, corpus string) error {
	corpusLen := len(corpus)
	if corpusLen == 0 {
		return nil
	}
	if corpusLen%2 == 1 {
		return errors.New(ErrParseInvalidLength)
	}

	var data = []byte(corpus)
	var c byte
	hex := [2]byte{}
	var hexChar byte
	var isHexChar bool
	var hexIndex, uuidIndex, di int

	for i := 0; i < len(data); i++ {
		c = data[i]
		if c == '{' && i == 0 {
			continue
		}
		if c == '{' {
			return errors.New(ErrParseIllegalCharacter)
		}
		if c == '}' && i != len(data)-1 {
			return errors.New(ErrParseIllegalCharacter)
		}
		if c == '}' {
			continue
		}

		if c == '-' && !(di == 8 || di == 12 || di == 16 || di == 20) {
			return errors.New(ErrParseIllegalCharacter)
		}
		if c == '-' {
			continue
		}

		hexChar, isHexChar = fromHexChar(c)
		if !isHexChar {
			return errors.New(ErrParseIllegalCharacter)
		}

		hex[hexIndex] = hexChar
		if hexIndex == 1 {
			if uuidIndex >= 16 {
				return errors.New(ErrParseInvalidLength)
			}
			(*uuid)[uuidIndex] = hex[0]<<4 | hex[1]
			uuidIndex++

			hexIndex = 0
		} else {
			hexIndex++
		}
		di++
	}
	if uuidIndex != 16 {
		return errors.New(ErrParseInvalidLength)
	}
	return nil
}

func fromHexChar(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}

	return 0, false
}
