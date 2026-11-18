package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSupportsComponentNetworkMap(t *testing.T) {
	assert.False(t, SupportsComponentNetworkMap(-1), "invalid version must get the full map")
	assert.False(t, SupportsComponentNetworkMap(int(Base)), "base peers must get the full map")
	assert.True(t, SupportsComponentNetworkMap(int(ComponentNetworkMap)), "component peers must get components")
	assert.True(t, SupportsComponentNetworkMap(int(ComponentNetworkMap)+1), "newer peers must get components")
}
