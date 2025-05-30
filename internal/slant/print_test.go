package slant

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrint(t *testing.T) {
	output, err := PrintString("WARDEN")
	require.NoError(t, err)
	require.NotEmpty(t, output)
}
