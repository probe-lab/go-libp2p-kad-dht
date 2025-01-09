package dht

import (
	"io"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/ants"
	"github.com/libp2p/go-libp2p-kad-dht/internal/net"
	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/network"

	"github.com/libp2p/go-msgio"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
)

var dhtStreamIdleTimeout = 1 * time.Minute

// ErrReadTimeout is an error that occurs when no message is read within the timeout period.
var ErrReadTimeout = net.ErrReadTimeout

// handleNewStream implements the network.StreamHandler
func (dht *IpfsDHT) handleNewStream(s network.Stream) {
	if dht.handleNewMessage(s) {
		// If we exited without error, close gracefully.
		_ = s.Close()
	} else {
		// otherwise, send an error.
		_ = s.Reset()
	}
}

// Returns true on orderly completion of writes (so we can Close the stream).
func (dht *IpfsDHT) handleNewMessage(s network.Stream) bool {
	ctx := dht.ctx
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	mPeer := s.Conn().RemotePeer()

	timer := time.AfterFunc(dhtStreamIdleTimeout, func() { _ = s.Reset() })
	defer timer.Stop()

	for {
		if dht.getMode() != modeServer {
			logger.Debugf("ignoring incoming dht message while not in server mode")
			return false
		}

		var req pb.Message
		msgbytes, err := r.ReadMsg()
		msgLen := len(msgbytes)
		if err != nil {
			r.ReleaseMsg(msgbytes)
			if err == io.EOF {
				return true
			}
			// This string test is necessary because there isn't a single stream reset error
			// instance	in use.
			if c := baseLogger.Check(zap.DebugLevel, "error reading message"); c != nil && err.Error() != "stream reset" {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			if msgLen > 0 {
				_ = stats.RecordWithTags(ctx,
					[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
					metrics.ReceivedMessages.M(1),
					metrics.ReceivedMessageErrors.M(1),
					metrics.ReceivedBytes.M(int64(msgLen)),
				)
			}
			return false
		}
		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			if c := baseLogger.Check(zap.DebugLevel, "error unmarshaling message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			_ = stats.RecordWithTags(ctx,
				[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
				metrics.ReceivedMessages.M(1),
				metrics.ReceivedMessageErrors.M(1),
				metrics.ReceivedBytes.M(int64(msgLen)),
			)
			return false
		}

		timer.Reset(dhtStreamIdleTimeout)

		startTime := time.Now()
		ctx, _ := tag.New(ctx,
			tag.Upsert(metrics.KeyMessageType, req.GetType().String()),
		)

		agentVersion := ""
		val, err := dht.peerstore.Get(mPeer, "AgentVersion")
		if err == nil {
			agentVersion = val.(string)
		}

		maddrs := dht.peerstore.Addrs(mPeer)
		protocolIDs, _ := dht.peerstore.GetProtocols(mPeer) // ignore error

		// Sometimes we can get agent versions and protocols from the peer store
		// but there are no addresses. If there are no addresses in the peer
		// store, we at least use the one from this connection.
		if len(maddrs) == 0 {
			maddrs = append(maddrs, s.Conn().RemoteMultiaddr())
		}

		select {
		case dht.requestsLogChan <- ants.RequestEvent{
			Timestamp: startTime,
			Self:      dht.self,
			Remote:    mPeer,
			Type:      req.GetType(),
			Target:    req.GetKey(),
			// Transport:    s.Conn().ConnState().Transport,
			// Security:     s.Conn().ConnState().Security,
			// Muxer:        s.Conn().ConnState().StreamMultiplexer,
			AgentVersion: agentVersion,
			Protocols:    protocolIDs,
			Maddrs:       maddrs,
		}:
		default:
			baseLogger.Check(zap.ErrorLevel, "failed to log request, queue full")
		}

		stats.Record(ctx,
			metrics.ReceivedMessages.M(1),
			metrics.ReceivedBytes.M(int64(msgLen)),
		)

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "can't handle received message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())))
			}
			return false
		}

		if c := baseLogger.Check(zap.DebugLevel, "handling message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()))
		}
		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "error handling message"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		if c := baseLogger.Check(zap.DebugLevel, "handled message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", time.Since(startTime)))
		}

		if resp == nil {
			continue
		}

		// send out response msg
		err = net.WriteMsg(s, resp)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "error writing response"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		elapsedTime := time.Since(startTime)

		if c := baseLogger.Check(zap.DebugLevel, "responded to message"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", elapsedTime))
		}

		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)
		stats.Record(ctx, metrics.InboundRequestLatency.M(latencyMillis))
	}
}
