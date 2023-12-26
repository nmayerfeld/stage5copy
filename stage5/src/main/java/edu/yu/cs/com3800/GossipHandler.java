package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipHandler extends Thread implements LoggingServer {
    public class FailRunnable implements Runnable,LoggingServer{
        long serverID;
        private ConcurrentHashMap<Long, HeartbeatTableEntry> htEntries;
        private ConcurrentHashMap<Long, ScheduledFuture> failOrCleanupFutureToServerID;
        private HeartbeatTableEntry myEntry;
        private ScheduledExecutorService es;
        private volatile ZooKeeperPeerServerImpl zkpsi;
        private Logger l;
        private long cleanup;
        private long fail;
        public FailRunnable(long serverID, ZooKeeperPeerServerImpl zkpsi,HeartbeatTableEntry myEntry, ConcurrentHashMap<Long,HeartbeatTableEntry> htEntries, ConcurrentHashMap<Long,ScheduledFuture>failOrCleanupFutureToServerID, ScheduledExecutorService es, Logger l, long cleanup,long fail){
            this.serverID=serverID;
            this.htEntries=htEntries;
            this.failOrCleanupFutureToServerID=failOrCleanupFutureToServerID;
            this.myEntry=myEntry;
            this.es=es;
            this.l=l;
            this.fail=fail;
            this.cleanup=cleanup;
            this.zkpsi=zkpsi;
        }
        @Override
        public void run() {
            if(!Thread.currentThread().isInterrupted()){
                this.l.log(Level.FINEST,"FailRunnable for server: "+serverID+" hit time to fail at: "+System.currentTimeMillis()+", replacing entry with one marked failed and creating the cleanup Runnable and ScheduledFuture");
                this.zkpsi.reportFailedPeer(serverID);
                HeartbeatTableEntry oldEntry=this.htEntries.get(serverID);
                HeartbeatTableEntry newEntry=new HeartbeatTableEntry(serverID,oldEntry.getHeartbeatCounter(),oldEntry.getLocalTime(),true);
                this.htEntries.replace(serverID,newEntry);
                this.l.log(Level.FINEST,"replaced oldEntry: "+oldEntry.toString()+" with new entry: "+newEntry.toString());
                CleanupRunnable cr=new CleanupRunnable(serverID,zkpsi,newEntry,this.htEntries,this.failOrCleanupFutureToServerID,l);
                this.l.log(Level.FINEST,"creating scheduled future with timeout in: "+(cleanup-fail));
                this.l.log(Level.FINEST,"current time is: "+System.currentTimeMillis()+"  \nshould cleanup at: "+(System.currentTimeMillis()+cleanup-fail));
                ScheduledFuture<?> sf=es.schedule(cr,cleanup-fail, TimeUnit.MILLISECONDS);
                this.failOrCleanupFutureToServerID.put(serverID,sf);
            }
        }
    }
    public class CleanupRunnable implements Runnable,LoggingServer {
        long serverID;
        private ConcurrentHashMap<Long, HeartbeatTableEntry> htEntries;
        private ConcurrentHashMap<Long, ScheduledFuture> failOrCleanupFutureToServerID;
        private volatile ZooKeeperPeerServerImpl zkpsi;
        private HeartbeatTableEntry myEntry;
        private Logger l;
        public CleanupRunnable(long serverID, ZooKeeperPeerServerImpl zkpsi,HeartbeatTableEntry myEntry, ConcurrentHashMap<Long, HeartbeatTableEntry> htEntries, ConcurrentHashMap<Long, ScheduledFuture> failOrCleanupFutureToServerID, Logger l) {
            this.serverID = serverID;
            this.htEntries = htEntries;
            this.failOrCleanupFutureToServerID = failOrCleanupFutureToServerID;
            this.myEntry = myEntry;
            this.zkpsi=zkpsi;
            this.l=l;
        }

        @Override
        public void run() {
            if (!Thread.currentThread().isInterrupted()) {
                this.l.log(Level.FINEST,"Cleanup Runnable for server: "+serverID+" hit time to cleanup, removing entry: "+this.htEntries.get(serverID)+" from table, marking as dead in zkpsi, removing sf entry as well");
                this.htEntries.remove(serverID);
                this.failOrCleanupFutureToServerID.remove(serverID);
            }
        }
    }
    public class SendGossipToRandomEveryTGossipSeconds extends Thread implements LoggingServer{
        private Random r;
        private volatile ZooKeeperPeerServerImpl zkpsi;
        private LinkedBlockingQueue<Message> outgoingMessages;
        private ConcurrentHashMap<Long, HeartbeatTableEntry> heartbeatTableEntryMap;
        private ConcurrentHashMap<Long, ScheduledFuture> cleanupAndFailFutureTracker;
        private long cleanup;
        private long fail;
        private ScheduledExecutorService es;
        private Logger l;
        private long tGossip;
        private long serverID;
        public SendGossipToRandomEveryTGossipSeconds(long serverID, ZooKeeperPeerServerImpl zkpsi,long tGossip,LinkedBlockingQueue<Message> outgoingMessages,ConcurrentHashMap<Long,HeartbeatTableEntry> heartbeatTableEntryMap,ConcurrentHashMap<Long,ScheduledFuture> cleanupAndFailFutureTracker,ScheduledExecutorService es, long fail,long cleanup,Logger l){
            this.r=new Random();
            this.outgoingMessages=outgoingMessages;
            this.heartbeatTableEntryMap=heartbeatTableEntryMap;
            this.cleanupAndFailFutureTracker=cleanupAndFailFutureTracker;
            this.zkpsi=zkpsi;
            this.l=l;
            this.tGossip=tGossip;
            this.serverID=serverID;
            this.cleanup=cleanup;
            this.fail=fail;
            this.es=es;
            this.setDaemon(true);
            this.setName("Gossip Sender for server: "+serverID);
        }
        public void shutdown(){
            this.l.log(Level.WARNING,"GossipSender.shutdown() called");
            interrupt();
            es.shutdown();
            this.zkpsi=null;
            this.outgoingMessages=null;
            this.heartbeatTableEntryMap=null;
            this.cleanupAndFailFutureTracker=null;
        }
        @Override
        public void run() {
            while(!isInterrupted()){
                try {
                    Thread.sleep(tGossip);
                    this.incrementMyHeartbeat();
                    InetSocketAddress destination=this.zkpsi.getRandomUDPPeer();
                    Message myGossip=new Message(Message.MessageType.GOSSIP,buildMsgContent(Collections.unmodifiableMap(this.heartbeatTableEntryMap),this.zkpsi.getServerId()), this.zkpsi.getAddress().getHostString(), this.zkpsi.getAddress().getPort(), destination.getHostString(), destination.getPort());
                    this.outgoingMessages.put(myGossip);
                    this.l.log(Level.FINEST,"sending gossip message: "+myGossip.toString());
                } catch (InterruptedException e) {
                    l.log(Level.WARNING, "interrupted while waiting tGossip");
                    Thread.currentThread().interrupt();
                    continue;
                }
            }
            this.l.log(Level.WARNING,"Exiting gossipSender.run()");
            this.es.shutdown();
        }
        private void incrementMyHeartbeat(){
            HeartbeatTableEntry oldEntry=this.heartbeatTableEntryMap.get(serverID);
            ScheduledFuture oldFuture=this.cleanupAndFailFutureTracker.get(serverID);
            if(oldFuture==null){
                this.l.log(Level.WARNING,"somehow the future for server: "+serverID+"got lost from the table");
            }
            else{
                this.l.log(Level.FINEST,"replacing entry for server: "+serverID+" into tables, cancelling old future,and dealing with setting up a new fail future");
                oldFuture.cancel(false);
            }
            HeartbeatTableEntry newEntry=new HeartbeatTableEntry(serverID,oldEntry.getHeartbeatCounter()+1,System.currentTimeMillis()/1000,false);
            this.heartbeatTableEntryMap.replace(serverID,newEntry);
            FailRunnable fr=new FailRunnable(serverID,zkpsi,newEntry,this.heartbeatTableEntryMap,this.cleanupAndFailFutureTracker,es,logger,CLEANUP,FAIL);
            ScheduledFuture<?> failOrCleanupFuture=es.schedule(fr,FAIL,TimeUnit.MILLISECONDS);
            this.cleanupAndFailFutureTracker.put(serverID,failOrCleanupFuture);
        }
    }
    private static final int GOSSIP = 3000;
    private static final int FAIL = GOSSIP * 15;
    private static final int CLEANUP = FAIL * 2;
    private volatile ZooKeeperPeerServerImpl zkpsi;
    private ConcurrentHashMap<Long,HeartbeatTableEntry> heartbeatTableEntryMap;
    private ConcurrentHashMap<Long,ScheduledFuture> cleanupAndFailFutureTracker;
    private LinkedBlockingQueue<Message> incomingGossipMessages;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private SendGossipToRandomEveryTGossipSeconds sendGossip;
    private ScheduledExecutorService es;
    private Logger logger;
    private boolean shutdown=false;
    public GossipHandler(LinkedBlockingQueue<Message> incomingGossipMessages, LinkedBlockingQueue<Message> outgoingMessages, ZooKeeperPeerServerImpl zkpsi){
        this.setDaemon(true);
        this.setName("GossipHandler for server: "+zkpsi.getServerId());
        this.incomingGossipMessages=incomingGossipMessages;
        this.outgoingMessages=outgoingMessages;
        this.zkpsi=zkpsi;
        this.heartbeatTableEntryMap=new ConcurrentHashMap<>();
        this.cleanupAndFailFutureTracker=new ConcurrentHashMap<>();
        this.es=Executors.newScheduledThreadPool(2);
        this.populateTrackersForGossip();
        try {
            this.logger=initializeLogging("logs/GossipHandler-Logs",GossipHandler.class.getCanonicalName() + "-on-server-with-ID-" + this.zkpsi.getServerId());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.sendGossip=new SendGossipToRandomEveryTGossipSeconds(this.zkpsi.getServerId(),zkpsi,GOSSIP,this.outgoingMessages,heartbeatTableEntryMap,cleanupAndFailFutureTracker,es,FAIL,CLEANUP,logger);
        this.sendGossip.setName("send-gossip-for-gh-on-server-"+this.zkpsi.getServerId());
        this.sendGossip.setDaemon(true);
    }
    @Override
    public void run(){
        this.sendGossip.start();
        while(!this.shutdown){
            try{
                Message m=incomingGossipMessages.take();
                if(m.getMessageType()!= Message.MessageType.GOSSIP){
                    this.logger.log(Level.WARNING,"message in gossip queue was not gossip");
                }
                GossipNotification gn=getHTEntriesFromMessage(m);
                this.logger.log(Level.FINEST,"at time: "+System.currentTimeMillis()/1000+"received gossip notification: "+gn.toString());
                Map<Long,HeartbeatTableEntry> htEntries=gn.getHtEntries();
                long serverIdOfSender=gn.getServerIDofSender();
                for(Long serverId: htEntries.keySet()) {
                    HeartbeatTableEntry htEntry = htEntries.get(serverId);
                    //check if current node has no record of it in table - it's dead, then move on if dead
                    if (this.zkpsi.isPeerDead(serverId)) {
                        continue;
                    }
                    //this is the first time it's getting into the table
                    else if (this.heartbeatTableEntryMap.get(serverId) == null) {
                        this.logger.log(Level.FINEST, "adding entry for server: " + serverId + " into tables and dealing with setting up a fail future");
                        HeartbeatTableEntry newEntry = new HeartbeatTableEntry(serverId, htEntry.getHeartbeatCounter(), System.currentTimeMillis() / 1000, false);
                        this.heartbeatTableEntryMap.put(serverId, newEntry);
                        FailRunnable fr = new FailRunnable(serverId, zkpsi, newEntry, this.heartbeatTableEntryMap, this.cleanupAndFailFutureTracker, es, logger, CLEANUP, FAIL);
                        ScheduledFuture<?> failOrCleanupFuture = es.schedule(fr, FAIL, TimeUnit.MILLISECONDS);
                        this.cleanupAndFailFutureTracker.put(serverId, failOrCleanupFuture);
                    } else if (this.heartbeatTableEntryMap.get(serverId).shouldBeReplacedBy(htEntry, serverIdOfSender)) {
                        ScheduledFuture oldFuture = this.cleanupAndFailFutureTracker.get(serverId);
                        if (oldFuture == null) {
                            this.logger.log(Level.WARNING, "somehow the future for server: " + serverId + "got lost from the table");
                        }
                        this.logger.log(Level.FINEST, "replacing entry for server: " + serverId + " into tables, cancelling old future,and dealing with setting up a new fail future");
                        oldFuture.cancel(false);
                        HeartbeatTableEntry newEntry = new HeartbeatTableEntry(serverId, htEntry.getHeartbeatCounter(), System.currentTimeMillis() / 1000, false);
                        this.heartbeatTableEntryMap.replace(serverId, newEntry);
                        FailRunnable fr = new FailRunnable(serverId, zkpsi, newEntry, this.heartbeatTableEntryMap, this.cleanupAndFailFutureTracker, es, logger, CLEANUP, FAIL);
                        ScheduledFuture<?> failOrCleanupFuture = es.schedule(fr, FAIL, TimeUnit.MILLISECONDS);
                        this.cleanupAndFailFutureTracker.put(serverId, failOrCleanupFuture);
                    }
                }
            } catch (Exception e){
                if(this.shutdown){
                    this.logger.log(Level.SEVERE,"exception "+e.toString()+"thrown during shutdown. Shutting down.");
                    Thread.currentThread().interrupt();
                }
                else{
                    this.logger.log(Level.WARNING,"exception thrown. ",e);
                }
            }
        }
        this.logger.log(Level.WARNING,"exiting GossipHandler.run()");
    }
    public void shutdown(){
        this.shutdown=true;
        this.logger.log(Level.WARNING, "GossipHandler.shutdown called");
        this.sendGossip.interrupt();
        for(Long l:this.cleanupAndFailFutureTracker.keySet()){
            if(this.cleanupAndFailFutureTracker.get(l)!=null){
                this.cleanupAndFailFutureTracker.get(l).cancel(true);
                this.cleanupAndFailFutureTracker.remove(l);
            }
        }
        this.es.shutdown();
        interrupt();
    }
    private void populateTrackersForGossip(){
        Set<Long> serverIDsAliveAtStart=this.zkpsi.getServerIDsOfAlivePeers();
        Set<Long> withAdd=new HashSet<>(serverIDsAliveAtStart);
        withAdd.add(this.zkpsi.getServerId());
        for(long id:withAdd){
            HeartbeatTableEntry entry=new HeartbeatTableEntry(id,1L,System.currentTimeMillis()/1000,false);
            this.heartbeatTableEntryMap.put(id,new HeartbeatTableEntry(id,1L,System.currentTimeMillis()/1000,false));
            FailRunnable fr=new FailRunnable(id,zkpsi,entry,this.heartbeatTableEntryMap,this.cleanupAndFailFutureTracker,es,logger,CLEANUP,FAIL);
            ScheduledFuture<?> failOrCleanupFuture=es.schedule(fr,FAIL,TimeUnit.MILLISECONDS);
            this.cleanupAndFailFutureTracker.put(id,failOrCleanupFuture);
        }
    }
    protected static GossipNotification getHTEntriesFromMessage(Message m){
        Map<Long,HeartbeatTableEntry> results=new HashMap();
        ByteBuffer buffer = ByteBuffer.wrap(m.getMessageContents());
        buffer.clear();
        long serverIdOfSender=buffer.getLong();
        int numEntries=buffer.getInt();
        for(int i=0;i<numEntries;i++){
            long serverID= buffer.getLong();
            long localTime =buffer.getLong();
            long heartbeatCounter=buffer.getLong();
            char tf=buffer.getChar();
            boolean failed;
            if(tf=='T'){
                failed=true;
            }
            else if(tf=='F'){
                failed=false;
            }
            else{
                throw new IllegalStateException("wrong value for tf");
            }
            results.put(serverID,new HeartbeatTableEntry(serverID,heartbeatCounter,localTime,failed));
        }
        return new GossipNotification(results,serverIdOfSender);
    }
    protected static byte[] buildMsgContent(Map<Long,HeartbeatTableEntry> htEntries, long serverIdOfSender){
        // size of buffer = 8 bytes (for long id of sender) + 4 bytes (for int storing numEntries) + size of HeartbeatTable*26
        // size of individual HeartbeatTableEntry =26 bytes
        //        1 long (serverID) = 8 bytes
        //        1 long (localTime) = 8 bytes
        //        1 long (heartbeatCounter) = 8 bytes
        //        1 char (tf) = 2 bytes
        int bufferSize=12+htEntries.keySet().size()*26;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.clear();
        buffer.putLong(serverIdOfSender);
        buffer.putInt(htEntries.keySet().size());
        for(Long serverId:htEntries.keySet()){
            HeartbeatTableEntry htEntry=htEntries.get(serverId);
            buffer.putLong(htEntry.getServerId());
            buffer.putLong(htEntry.getLocalTime());
            buffer.putLong(htEntry.getHeartbeatCounter());
            if(htEntry.getFailed()){
                buffer.putChar('T');
            }
            else{
                buffer.putChar('F');
            }
        }
        buffer.flip();
        return buffer.array();
    }
}
