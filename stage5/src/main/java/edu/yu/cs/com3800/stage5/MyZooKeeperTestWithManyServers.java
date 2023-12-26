package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MyZooKeeperTestWithManyServers {
    public static void main(String[] args) {
        tradeMessages();
    }

    public static void tradeMessages() {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
        peerIDtoAddress.put(9L, new InetSocketAddress("localhost", 8090));
        peerIDtoAddress.put(10L, new InetSocketAddress("localhost", 9000));
        peerIDtoAddress.put(11L, new InetSocketAddress("localhost", 9010));
        peerIDtoAddress.put(12L, new InetSocketAddress("localhost", 9020));
        peerIDtoAddress.put(13L, new InetSocketAddress("localhost", 9030));
        peerIDtoAddress.put(14L, new InetSocketAddress("localhost", 9040));
        peerIDtoAddress.put(15L, new InetSocketAddress("localhost", 9050));
        peerIDtoAddress.put(16L, new InetSocketAddress("localhost", 9060));

        //create servers
        ArrayList<ZooKeeperPeerServer> servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map,1);
            servers.add(server);
            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
        }
        //wait for threads to start
        try {
            Thread.sleep(2500);
        }
        catch (Exception e) {
        }
        //print out the leaders and shutdown
        for (ZooKeeperPeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
            server.shutdown();
        }
    }
}
