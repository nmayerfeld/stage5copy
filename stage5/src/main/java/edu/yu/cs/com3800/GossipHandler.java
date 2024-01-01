package edu.yu.cs.com3800;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GossipHandler extends Thread implements LoggingServer {
    public class LogHandler implements HttpHandler {
        private String type;
        private long id;
        public LogHandler(String type, long id){
            this.type=type;
            this.id=id;
        }
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String filePath="";
            if(this.type.equals("verbose")){
                filePath="logs/GossipHandler-Logs/server-"+id+"/verboseLog.log";
            }
            else if(this.type.equals("summary")){
                filePath="logs/GossipHandler-Logs/server-"+id+"/summaryLog.log";
            }
            File file = new File(filePath);
            // Check if the file exists
            if (file.exists() && file.isFile()) {
                // Set the Content-Type header based on the file type
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                // Set the Content-Disposition header to prompt download
                exchange.getResponseHeaders().set("Content-Disposition", "attachment; filename=" + file.getName());
                // Get the response output stream
                byte[] fileContents=Files.readAllBytes(Paths.get(filePath));
                // Copy the file content to the response output stream
                exchange.sendResponseHeaders(200, fileContents.length);
                OutputStream os = exchange.getResponseBody();
                os.write(fileContents);
                os.close();
            } else {
                String response = "couldn't find the file";
                // If the file doesn't exist, send a not found response code (404)
                exchange.sendResponseHeaders(400,response.getBytes(StandardCharsets.UTF_8).length);
                OutputStream os= exchange.getResponseBody();
                os.write(response.getBytes(StandardCharsets.UTF_8));
                os.close();
            }
            // Close the exchange
            exchange.close();
        }
    }
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
                long id=this.zkpsi.getServerId();
                this.l.log(Level.FINEST,id+": no heartbeat from server "+serverID+" - SERVER FAILED\n");
                System.out.println(id+": no heartbeat from server "+serverID+" - SERVER FAILED\n");
                ZooKeeperPeerServer.ServerState state=this.zkpsi.getPeerState();
                ZooKeeperPeerServer.ServerState newState;
                if(state== ZooKeeperPeerServer.ServerState.OBSERVER){
                    newState= ZooKeeperPeerServer.ServerState.OBSERVER;
                }
                else if(this.zkpsi.getCurrentLeader().getProposedLeaderID()==serverID){
                    newState= ZooKeeperPeerServer.ServerState.LOOKING;
                }
                else{
                    newState= ZooKeeperPeerServer.ServerState.FOLLOWING;
                }
                System.out.println(id+": switching from "+state+" to "+newState+"\n");
                this.l.log(Level.FINEST,id+": switching from "+state+" to "+newState+"\n");
                this.zkpsi.reportFailedPeer(serverID);
                HeartbeatTableEntry oldEntry=this.htEntries.get(serverID);
                HeartbeatTableEntry newEntry=new HeartbeatTableEntry(serverID,oldEntry.getHeartbeatCounter(),oldEntry.getLocalTime(),true);
                this.htEntries.replace(serverID,newEntry);
                //this.l.log(Level.FINEST,"replaced oldEntry: "+oldEntry.toString()+" with new entry: "+newEntry.toString());
                CleanupRunnable cr=new CleanupRunnable(serverID,zkpsi,newEntry,this.htEntries,this.failOrCleanupFutureToServerID,l);
                //this.l.log(Level.FINEST,"creating scheduled future with timeout in: "+(cleanup-fail));
                //this.l.log(Level.FINEST,"current time is: "+System.currentTimeMillis()+"  \nshould cleanup at: "+(System.currentTimeMillis()+cleanup-fail));
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
                this.l.log(Level.FINEST,"Cleanup Runnable for server: "+serverID+" hit time to cleanup, removing entry: \n"+this.htEntries.get(serverID)+"\n from table, marking as dead in zkpsi, removing sf entry as well\n");
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
        private Logger summaryLogFile;
        public SendGossipToRandomEveryTGossipSeconds(long serverID, ZooKeeperPeerServerImpl zkpsi,long tGossip,LinkedBlockingQueue<Message> outgoingMessages,ConcurrentHashMap<Long,HeartbeatTableEntry> heartbeatTableEntryMap,ConcurrentHashMap<Long,ScheduledFuture> cleanupAndFailFutureTracker,ScheduledExecutorService es, long fail,long cleanup,Logger l, Logger summaryLogFile){
            this.r=new Random();
            this.outgoingMessages=outgoingMessages;
            this.heartbeatTableEntryMap=heartbeatTableEntryMap;
            this.cleanupAndFailFutureTracker=cleanupAndFailFutureTracker;
            this.zkpsi=zkpsi;
            this.l=l;
            this.summaryLogFile=summaryLogFile;
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
            FailRunnable fr=new FailRunnable(serverID,zkpsi,newEntry,this.heartbeatTableEntryMap,this.cleanupAndFailFutureTracker,es,summaryLogFile,CLEANUP,FAIL);
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
    private Logger verboseLogger;
    private Logger summaryLogFile;
    private Logger debuggerLogger;
    private boolean shutdown=false;
    private HttpServer serverForLogs;
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
            this.verboseLogger=initializeLogging("logs/GossipHandler-Logs/server-"+this.zkpsi.getServerId(),"verboseLog");
            this.summaryLogFile=initializeLogging("logs/GossipHandler-Logs/server-"+this.zkpsi.getServerId(),"summaryLog");
            this.debuggerLogger=initializeLogging("logs/GossipHandler-Logs/server-"+this.zkpsi.getServerId(),"debugLog");
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.sendGossip=new SendGossipToRandomEveryTGossipSeconds(this.zkpsi.getServerId(),zkpsi,GOSSIP,this.outgoingMessages,heartbeatTableEntryMap,cleanupAndFailFutureTracker,es,FAIL,CLEANUP,debuggerLogger,summaryLogFile);
        this.sendGossip.setName("send-gossip-for-gh-on-server-"+this.zkpsi.getServerId());
        this.sendGossip.setDaemon(true);
        int port=4444+this.zkpsi.getServerId().intValue();
        while(true){
            try {
                this.serverForLogs=HttpServer.create(new InetSocketAddress(port),0);
                break;
            } catch (IOException e) {
                this.debuggerLogger.log(Level.WARNING,"failed to create server for logs");
            }
        }
        ThreadPoolExecutor tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        this.serverForLogs.setExecutor(tpe);
        this.serverForLogs.createContext("/verbose", new LogHandler("verbose",this.zkpsi.getServerId()));
        this.serverForLogs.createContext("/summary", new LogHandler("summary",this.zkpsi.getServerId()));
    }
    @Override
    public void run(){
        this.serverForLogs.start();
        this.sendGossip.start();
        while(!this.shutdown){
            try{
                Message m=incomingGossipMessages.take();
                if(m.getMessageType()!= Message.MessageType.GOSSIP){
                    this.debuggerLogger.log(Level.WARNING,"message in gossip queue was not gossip");
                }
                GossipNotification gn=getHTEntriesFromMessage(m);
                this.verboseLogger.log(Level.FINEST,"at time: "+System.currentTimeMillis()/1000+"received gossip notification: "+gn.toString());
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
                        this.debuggerLogger.log(Level.FINEST, "adding entry for server: " + serverId + " into tables and dealing with setting up a fail future");
                        HeartbeatTableEntry newEntry = new HeartbeatTableEntry(serverId, htEntry.getHeartbeatCounter(), System.currentTimeMillis() / 1000, false);
                        this.heartbeatTableEntryMap.put(serverId, newEntry);
                        FailRunnable fr = new FailRunnable(serverId, zkpsi, newEntry, this.heartbeatTableEntryMap, this.cleanupAndFailFutureTracker, es, summaryLogFile, CLEANUP, FAIL);
                        ScheduledFuture<?> failOrCleanupFuture = es.schedule(fr, FAIL, TimeUnit.MILLISECONDS);
                        this.cleanupAndFailFutureTracker.put(serverId, failOrCleanupFuture);
                    } else if (this.heartbeatTableEntryMap.get(serverId).shouldBeReplacedBy(htEntry, serverIdOfSender)) {
                        ScheduledFuture oldFuture = this.cleanupAndFailFutureTracker.get(serverId);
                        if (oldFuture == null) {
                            this.debuggerLogger.log(Level.WARNING, "somehow the future for server: " + serverId + "got lost from the table");
                        }
                        this.debuggerLogger.log(Level.FINEST, "replacing entry for server: " + serverId + " into tables, cancelling old future,and dealing with setting up a new fail future");
                        this.summaryLogFile.log(Level.FINEST,this.zkpsi.getServerId()+": updated "+serverId+"’s heartbeat sequence to "+htEntry.getHeartbeatCounter()+" based on message from "+serverIdOfSender+" at node time "+System.currentTimeMillis());
                        oldFuture.cancel(false);
                        HeartbeatTableEntry newEntry = new HeartbeatTableEntry(serverId, htEntry.getHeartbeatCounter(), System.currentTimeMillis() / 1000, false);
                        this.heartbeatTableEntryMap.replace(serverId, newEntry);
                        FailRunnable fr = new FailRunnable(serverId, zkpsi, newEntry, this.heartbeatTableEntryMap, this.cleanupAndFailFutureTracker, es, summaryLogFile, CLEANUP, FAIL);
                        ScheduledFuture<?> failOrCleanupFuture = es.schedule(fr, FAIL, TimeUnit.MILLISECONDS);
                        this.cleanupAndFailFutureTracker.put(serverId, failOrCleanupFuture);
                    }
                }
            } catch (Exception e){
                if(this.shutdown){
                    this.debuggerLogger.log(Level.SEVERE,"exception "+e.toString()+"thrown during shutdown. Shutting down.");
                    Thread.currentThread().interrupt();
                }
                else{
                    this.debuggerLogger.log(Level.WARNING,"exception thrown. ",e);
                }
            }
        }
        this.debuggerLogger.log(Level.WARNING,"exiting GossipHandler.run()");
    }
    public void shutdown(){
        this.shutdown=true;
        this.debuggerLogger.log(Level.WARNING, "GossipHandler.shutdown called");
        this.sendGossip.interrupt();
        for(Long l:this.cleanupAndFailFutureTracker.keySet()){
            if(this.cleanupAndFailFutureTracker.get(l)!=null){
                this.cleanupAndFailFutureTracker.get(l).cancel(true);
                this.cleanupAndFailFutureTracker.remove(l);
            }
        }
        this.es.shutdown();
        this.serverForLogs.stop(0);
        interrupt();
    }
    private void populateTrackersForGossip(){
        Set<Long> serverIDsAliveAtStart=this.zkpsi.getServerIDsOfAlivePeers();
        Set<Long> withAdd=new HashSet<>(serverIDsAliveAtStart);
        withAdd.add(this.zkpsi.getServerId());
        for(long id:withAdd){
            HeartbeatTableEntry entry=new HeartbeatTableEntry(id,1L,System.currentTimeMillis()/1000,false);
            this.heartbeatTableEntryMap.put(id,new HeartbeatTableEntry(id,1L,System.currentTimeMillis()/1000,false));
            FailRunnable fr=new FailRunnable(id,zkpsi,entry,this.heartbeatTableEntryMap,this.cleanupAndFailFutureTracker,es,summaryLogFile,CLEANUP,FAIL);
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
