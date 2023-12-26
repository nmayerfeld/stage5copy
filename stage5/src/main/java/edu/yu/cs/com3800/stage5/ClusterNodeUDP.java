package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;

public class ClusterNodeUDP {
    private long id;
    private InetSocketAddress UDPAddress;
    public ClusterNodeUDP(long id, InetSocketAddress UDPAddress){
        this.id=id;
        this.UDPAddress=UDPAddress;
    }
    public long getId(){return this.id;}
    public InetSocketAddress getUDPAddress(){return this.UDPAddress;}
}
