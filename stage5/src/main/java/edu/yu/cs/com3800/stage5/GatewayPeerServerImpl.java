package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl{
    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress) {
        super(myPort, peerEpoch, id, peerIDtoAddress,id);
        super.setPeerState(ServerState.OBSERVER);
    }
    public void setPeerState(ServerState newState) {
        throw new UnsupportedOperationException("can't change the state of the server");
    }
}
