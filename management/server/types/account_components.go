package types

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	nbdns "github.com/netbirdio/netbird/dns"
	"github.com/netbirdio/netbird/management/internals/modules/zones"
	routerTypes "github.com/netbirdio/netbird/management/server/networks/routers/types"
	"github.com/netbirdio/netbird/management/server/telemetry"
)

func (a *Account) GetPeerNetworkMapFromComponents(
	ctx context.Context,
	peerID string,
	peersCustomZone nbdns.CustomZone,
	accountZones []*zones.Zone,
	validatedPeersMap map[string]struct{},
	resourcePolicies map[string][]*Policy,
	routers map[string]map[string]*routerTypes.NetworkRouter,
	metrics *telemetry.AccountManagerMetrics,
	groupIDToUserIDs map[string][]string,
) *NetworkMap {
	start := time.Now()

	components := a.GetPeerNetworkMapComponents(
		ctx,
		peerID,
		peersCustomZone,
		accountZones,
		validatedPeersMap,
		resourcePolicies,
		routers,
		groupIDToUserIDs,
	)

	if components.IsEmpty() {
		return &NetworkMap{Network: components.Network}
	}

	nm := CalculateNetworkMapFromComponents(ctx, components)

	if metrics != nil {
		objectCount := int64(len(nm.Peers) + len(nm.OfflinePeers) + len(nm.Routes) + len(nm.FirewallRules) + len(nm.RoutesFirewallRules))
		metrics.CountNetworkMapObjects(objectCount)
		metrics.CountGetPeerNetworkMapDuration(time.Since(start))

		if objectCount > 5000 {
			log.WithContext(ctx).Tracef("account: %s has a total resource count of %d objects from components, "+
				"peers: %d, offline peers: %d, routes: %d, firewall rules: %d, route firewall rules: %d",
				a.Id, objectCount, len(nm.Peers), len(nm.OfflinePeers), len(nm.Routes), len(nm.FirewallRules), len(nm.RoutesFirewallRules))
		}
	}

	return nm
}

// GetPeerNetworkMapComponents builds the account's slim twin store and computes
// the peer's components on it. The calculation itself lives on
// networkmap.NetworkMapData and never touches the Account.
func (a *Account) GetPeerNetworkMapComponents(
	ctx context.Context,
	peerID string,
	peersCustomZone nbdns.CustomZone,
	accountZones []*zones.Zone,
	validatedPeersMap map[string]struct{},
	resourcePolicies map[string][]*Policy,
	routers map[string]map[string]*routerTypes.NetworkRouter,
	groupIDToUserIDs map[string][]string,
) *NetworkMapComponents {

	nmd := a.toNetworkMapData(accountZones, validatedPeersMap, resourcePolicies, routers, groupIDToUserIDs)
	return nmd.GetPeerNetworkMapComponents(peerID, TwinCustomZone(peersCustomZone))
}

// PrecomputePostureValidation evaluates every posture check referenced by an enabled
// policy once and stores the results on the account, so the per-peer components
// calculations that follow look them up instead of re-evaluating checks for every
// peer pair. The evaluation itself runs on the twin store; every twin built from
// this account afterwards inherits the results. It must be called before the
// account is shared across goroutines.
func (a *Account) PrecomputePostureValidation(ctx context.Context) {
	nmd := a.toNetworkMapData(nil, nil, nil, nil, nil)
	nmd.PrecomputePostureValidation()
	a.PostureValidation = nmd.PostureValidation
}
