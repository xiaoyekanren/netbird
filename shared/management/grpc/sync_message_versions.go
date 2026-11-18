package grpc

// SyncMessageVersion identifies the wire format of the network map in a SyncResponse.
type SyncMessageVersion uint16

const (
	// Base carries the fully computed network map.
	Base SyncMessageVersion = iota
	// ComponentNetworkMap carries the network map components the client calculates locally.
	ComponentNetworkMap
)

// HighestSyncMessageVersion is the newest sync message version this build supports.
const HighestSyncMessageVersion = ComponentNetworkMap

// SupportsComponentNetworkMap reports whether a peer advertising peerVersion accepts the components network map.
func SupportsComponentNetworkMap(peerVersion int) bool {
	return peerVersion >= int(ComponentNetworkMap)
}
