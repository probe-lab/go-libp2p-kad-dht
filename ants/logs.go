package ants

import (
	"time"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

type RequestEvent struct {
	Timestamp    time.Time
	Self         peer.ID
	Remote       peer.ID
	Type         pb.Message_MessageType
	Target       mh.Multihash
	Transport    string
	Security     protocol.ID
	Muxer        protocol.ID
	AgentVersion string
	Protocols    []protocol.ID
	Maddrs       []ma.Multiaddr
}
