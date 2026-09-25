package app

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveNodeIDPersistsGeneratedIdentity(t *testing.T) {
	dataDir := t.TempDir()

	first, generated, err := ResolveNodeID(dataDir, "")
	require.NoError(t, err)
	assert.True(t, generated)
	assert.NotEqual(t, uuid.Nil, first)

	second, generated, err := ResolveNodeID(dataDir, "")
	require.NoError(t, err)
	assert.False(t, generated)
	assert.Equal(t, first, second)
}
