package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class ZKPSInstance {
    private ZooKeeperPeerServerImpl zkpsi;
    public static void main(String [] args){
        int myPort= Integer.parseInt(args[0]);
        long peerEpoch=Long.parseLong(args[1]);
        Map<Long, InetSocketAddress> peerIDToUDPAddress=getMapFromString(args[2]);
        long gatewayID=Long.parseLong(args[3]);
        long myID=Long.parseLong(args[4]);
        System.out.println("myID: "+myID);
        System.out.println("gatewayID: "+gatewayID);
        if(myID==gatewayID){
            System.out.println("starting the gateway gpsi and gateway server for id: "+gatewayID);
            GatewayPeerServerImpl gpsi=new GatewayPeerServerImpl(myPort,peerEpoch,myID,peerIDToUDPAddress);
            gpsi.start();
            GatewayServer gs=new GatewayServer(8090, gpsi);
            gs.start();
        }
        else{
            System.out.println("starting zkpsi for id: "+myID);
            ZooKeeperPeerServerImpl zkpsi=new ZooKeeperPeerServerImpl(myPort,peerEpoch,myID,peerIDToUDPAddress,gatewayID);
            zkpsi.start();
        }
    }
    private static Map<Long,InetSocketAddress> getMapFromString(String s){
        Map<Long,InetSocketAddress> results=new HashMap<>();
        String [] entries=s.split("\n");
        for(String entry: entries){
            String [] piecesOfEntry=entry.split(":");
            long id= Long.parseLong(piecesOfEntry[0]);
            String hostname=piecesOfEntry[1];
            int portNum=Integer.parseInt(piecesOfEntry[2]);
            results.put(id,new InetSocketAddress(hostname,portNum));
        }
        return results;
    }
}
