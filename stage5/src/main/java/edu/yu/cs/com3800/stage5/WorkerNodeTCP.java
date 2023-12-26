package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;

public class WorkerNodeTCP {
    private InetSocketAddress TCPAddress;
    private long id;
    public WorkerNodeTCP(long id, InetSocketAddress TCPAddress){
        this.id=id;
        this.TCPAddress=TCPAddress;
    }
    public long getId(){return this.id;}
    public InetSocketAddress getTCPAddress(){return this.TCPAddress;}
}
