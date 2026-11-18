package types_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/netbirdio/netbird/management/server/types"
)

func TestPeerNetworkMapResult_IsComponents(t *testing.T) {
	assert.True(t, types.PeerNetworkMapResult{Components: &types.NetworkMapComponents{}}.IsComponents())
	assert.False(t, types.PeerNetworkMapResult{NetworkMap: &types.NetworkMap{}}.IsComponents())
	assert.False(t, types.PeerNetworkMapResult{}.IsComponents())
}
