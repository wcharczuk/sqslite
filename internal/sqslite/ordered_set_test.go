package sqslite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_OrderedSet(t *testing.T) {
	os := NewOrderedSet[string]()
	os.Add("00")
	os.Add("01")
	os.Add("02")
	os.Add("03")
	os.Add("04")
	os.Add("05")

	require.True(t, os.Del("01"))
	require.True(t, os.Del("02"))
	require.False(t, os.Del("bogus"))

	require.Equal(t, 4, os.Len())
	require.True(t, os.Has("00"))
	require.True(t, os.Has("03"))
	require.True(t, os.Has("04"))
	require.True(t, os.Has("05"))

	var remaining []string
	for id := range os.InOrder() {
		remaining = append(remaining, id)
	}
	require.Equal(t, []string{"00", "03", "04", "05"}, remaining)
}
