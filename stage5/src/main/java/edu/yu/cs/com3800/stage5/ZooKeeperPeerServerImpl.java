package edu.yu.cs.com3800.stage5;
import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{
    private final InetSocketAddress myUDPAddress;
    private final InetSocketAddress myTCPAddress;
    private int indexOfNextWorker=0;
    private final int myUDPPort;
    private final int myTCPPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingElectionMessages;
    private LinkedBlockingQueue<Message> incomingGossipMessages;
    private ConcurrentHashMap<Long,byte[]> requestIdToCompletedWork;
    private Long id;
    private Logger logger;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoUDPAddress;
    private ConcurrentHashMap<InetSocketAddress,Long> UDPAddressToPeerID;
    private ConcurrentHashMap<Long,InetSocketAddress> peerIDtoTCPAddress;
    private ConcurrentHashMap<InetSocketAddress,Long> TCPAddressToPeerID;
    private List<Long> liveWorkers;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private GossipHandler gh;
    private RoundRobinLeader rrl;
    private JavaRunnerFollower jrf;
    private long gatewayID;
    private ZooKeeperLeaderElection zkle;
    private Random r=new Random();

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoUDPAddress, long gatewayID){
        //code here...
        this.myUDPAddress=new InetSocketAddress("localhost",myPort);
        this.myTCPAddress=new InetSocketAddress("localhost",myPort+2);
        this.requestIdToCompletedWork=new ConcurrentHashMap<>();
        this.liveWorkers=new ArrayList<>();
        this.myUDPPort=myPort;
        this.myTCPPort=myPort+2;
        this.peerEpoch=peerEpoch;
        this.id=id;
        this.gatewayID=gatewayID;
        this.peerIDtoUDPAddress=new ConcurrentHashMap<>(peerIDtoUDPAddress);
        this.UDPAddressToPeerID=new ConcurrentHashMap<>();
        this.peerIDtoTCPAddress=new ConcurrentHashMap<>();
        this.TCPAddressToPeerID=new ConcurrentHashMap<>();
        for(Long l:peerIDtoUDPAddress.keySet()){
            if(l!=this.gatewayID) {
                InetSocketAddress UDPAddress=peerIDtoUDPAddress.get(l);
                peerIDtoTCPAddress.put(l,new InetSocketAddress(UDPAddress.getHostString(),UDPAddress.getPort()+2));
                TCPAddressToPeerID.put(new InetSocketAddress(UDPAddress.getHostString(),UDPAddress.getPort()+2),l);
                this.liveWorkers.add(l);
            }
            UDPAddressToPeerID.put(peerIDtoUDPAddress.get(l),l);
        }
        this.state=ServerState.LOOKING;
        this.outgoingMessages=new LinkedBlockingQueue<>();
        this.incomingElectionMessages=new LinkedBlockingQueue<>();
        this.incomingGossipMessages=new LinkedBlockingQueue<>();
        this.senderWorker=new UDPMessageSender(this.outgoingMessages,this.myUDPPort);
        try {
            this.logger = initializeLogging("logs/ZKPSI-Logs",ZooKeeperPeerServer.class.getCanonicalName() + "-on-server-with-ID-" + this.id);
            this.receiverWorker=new UDPMessageReceiver(incomingElectionMessages,incomingGossipMessages,this.myUDPAddress,this.myUDPPort,this);
        } catch (IOException e) {
            this.logger.log(Level.SEVERE,"failed to create receiverWorker", e);
        }
        this.gh=new GossipHandler(incomingGossipMessages,outgoingMessages,this);
        this.setName("ZKPSI-ID-"+this.id);
        this.gh.setDaemon(true);
        this.zkle=new ZooKeeperLeaderElection(this,incomingElectionMessages);
    }

    @Override
    public void shutdown(){
        this.logger.log(Level.WARNING,"zkpsi.shutdown() called");
        this.shutdown = true;
        interrupt();
        if(rrl!=null){
            this.logger.log(Level.WARNING,"zkpsi.shutdown is shutting down its RRL ");
            rrl.shutdown();
        }
        if(jrf!=null){
            this.logger.log(Level.WARNING,"zkpsi.shutdown is shutting down its JRF ");
            jrf.shutdown();
        }
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        this.gh.shutdown();
    }

    @Override
    public synchronized void setCurrentLeader(Vote v){
        this.currentLeader=v;
    }

    @Override
    public synchronized Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        try {
            this.outgoingMessages.put(new Message(type, messageContents, myUDPAddress.getHostString(), myUDPPort, target.getHostString(), target.getPort()));
        } catch (InterruptedException e) {
            this.logger.log(Level.WARNING, "Exception caught while trying to add message to outgoing that sending to: "+target.getHostString(), e);
        }
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        this.logger.log(Level.FINE,"sending broadcast");
        for(InetSocketAddress isa:this.getUnmodifiableViewOfLiveNodesInCluster()){
            this.logger.log(Level.FINE,"sending to: "+isa.getPort());
            if(isa!=myUDPAddress){
                sendMessage(type,messageContents,isa);
            }
        }
    }

    @Override
    public synchronized ServerState getPeerState() {
        return this.state;
    }

    @Override
    public synchronized void setPeerState(ServerState newState) {
        this.state=newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public synchronized long getPeerEpoch() {
        return this.peerEpoch;
    }

    public synchronized void upEpoch(){this.peerEpoch++;}

    @Override
    public InetSocketAddress getAddress() {
        return this.myUDPAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myUDPPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoUDPAddress.get(peerId);
    }

    public long getGatewayID(){return this.gatewayID;}
    public InetSocketAddress getTCPPeerByID(long peerId) {
        return this.peerIDtoTCPAddress.get(peerId);
    }

    public WorkerNodeTCP getNextWorker(){
        long next=this.liveWorkers.get(indexOfNextWorker);
        indexOfNextWorker++;
        if(this.indexOfNextWorker>=this.liveWorkers.size()){
            indexOfNextWorker=0;
        }
        InetSocketAddress address=this.peerIDtoTCPAddress.get(next);
        while(address==null){
            next=this.liveWorkers.get(indexOfNextWorker);
            indexOfNextWorker++;
            if(this.indexOfNextWorker>=this.liveWorkers.size()){
                indexOfNextWorker=0;
            }
            address=this.peerIDtoTCPAddress.get(next);
        }
        return new WorkerNodeTCP(next,address);
    }
    public InetSocketAddress getRandomUDPPeer(){
        List<InetSocketAddress> serversUDPAddresses=List.copyOf(this.peerIDtoUDPAddress.values());
        int index=r.nextInt(serversUDPAddresses.size());
        return serversUDPAddresses.get(index);
    }
    @Override
    public int getQuorumSize() {
        int size=this.peerIDtoUDPAddress.size();
        this.logger.log(Level.FINE,"quorum size is: "+((size+1)/2+1));
        return (size+1)/2+1;
    }

    public List<InetSocketAddress> getUnmodifiableViewOfLiveWorkers(){
        return List.copyOf(this.peerIDtoTCPAddress.values());
    }
    public List<InetSocketAddress> getUnmodifiableViewOfLiveNodesInCluster(){
        return List.copyOf(this.peerIDtoUDPAddress.values());
    }
    @Override
    public void reportFailedPeer(long peerID) {
        //create a new map so that don't need to synchronize every single method that views this map
        // creating a new map and pointing the old reference at it is atomic!!!!
        //do the same for workers
        System.out.println("marking dead peer: "+peerID+" on server: "+this.id+"at time "+System.currentTimeMillis());
        this.logger.log(Level.FINE,"Hit report failed peer on server: "+this.id+" for failed server with id: "+peerID+"at time: "+System.nanoTime());

        this.UDPAddressToPeerID.remove(this.peerIDtoUDPAddress.get(peerID));
        this.peerIDtoUDPAddress.remove(peerID);
        this.TCPAddressToPeerID.remove(this.peerIDtoTCPAddress.get(peerID));
        this.peerIDtoTCPAddress.remove(peerID);

        if(this.getCurrentLeader().getProposedLeaderID()==peerID){
            if(this.getPeerState()==ServerState.FOLLOWING){
                this.setPeerState(ServerState.LOOKING);
            }
            this.upEpoch();
            this.zkle.clearOldInfoForNewElection();
            if(getPeerState()==ServerState.OBSERVER){
                this.currentLeader=null;
            }
            if(this.jrf!=null){
                this.jrf.shutdown();
            }
        }
    }

    @Override
    public boolean isPeerDead(long peerID) {
        InetSocketAddress isa=this.peerIDtoUDPAddress.get(peerID);
        if(isa==null&&this.id!=peerID){
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        Long peerIdOfAddress=this.UDPAddressToPeerID.get(address);
        if(peerIdOfAddress==null){return true;}
        else{
            return false;
        }
    }

    public Long getIdOfPeerFromTCPAddress(InetSocketAddress isa){
        return this.TCPAddressToPeerID.get(isa);
    }

    public Set<Long> getServerIDsOfAlivePeers(){
        return this.peerIDtoUDPAddress.keySet();
    }
    @Override
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        senderWorker.start();
        receiverWorker.start();
        gh.start();
        //step 2: create and run thread that listens for messages sent to this server
        //step 3: main server loop
        try{
            while (!this.shutdown){
                this.logger.log(Level.FINE,"iteration of while loop. State is: "+this.getPeerState());
                //if i was a follower, and my leader died, I just shut down my jrf and jrf.join() was called, so I'm iterating again.
                // however, my jrf will still be pointing at the terminated thread, so i must reset it to null in case I become the new Leader
                if(this.jrf!=null){
                    this.jrf=null;
                }
                if(getPeerState()==ServerState.LOOKING||(getPeerState()==ServerState.OBSERVER&&currentLeader==null)){
                    this.logger.log(Level.FINE,"hit line 241");
                    if(!this.shutdown){
                        this.setCurrentLeader(this.zkle.lookForLeader());
                    }
                }
                else if(getPeerState()==ServerState.FOLLOWING){
                    jrf=new JavaRunnerFollower(this.myTCPAddress, this.requestIdToCompletedWork,this);
                    this.logger.log(Level.FINEST,"created new JavaRunnerFollower");
                    jrf.setDaemon(true);
                    jrf.setName("JRF-ID-"+id);
                    jrf.start();
                    jrf.join();
                }
                else if(getPeerState()==ServerState.LEADING){
                    this.logger.log(Level.FINEST,"created new RoundRobinLeader");
                    rrl = new RoundRobinLeader(this.myTCPAddress,this,this.requestIdToCompletedWork);
                    rrl.setDaemon(true);
                    rrl.setName("RRL-ID-"+id);
                    rrl.start();
                    rrl.join();
                }
                else if(getPeerState()==ServerState.OBSERVER){
                    try{
                        Thread.sleep(1000);
                    } catch(InterruptedException e){
                        this.logger.log(Level.WARNING,"Observer interrupted while sleeping.  Shutting down");
                        this.shutdown();
                    }
                }
            }
            this.logger.log(Level.WARNING,"Exiting ZPKSI.run()");
        }
        catch (InterruptedException ex) {
            this.logger.log(Level.SEVERE, "JRF or RRL were interrupted while running.  Shutting down the PeerServerImpl");
            this.shutdown();
        }
    }

}