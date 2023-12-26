package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    class ResultListenerThread extends Thread implements LoggingServer{
        private LinkedBlockingQueue<WorkResult> workResultsQueue;
        private LinkedBlockingQueue<WorkRequest> workRequestsQueue;
        private Logger logger;
        private InetSocketAddress myTCPAddress;
        private volatile ZooKeeperPeerServerImpl zkpsi;
        private ConcurrentHashMap<Long,byte[]> requestIdToOrigMessageContents;
        public ResultListenerThread(LinkedBlockingQueue<WorkResult> workResultsQueue, LinkedBlockingQueue<WorkRequest>workRequestsQueue,InetSocketAddress myTCPAddress, ZooKeeperPeerServerImpl zkpsi, ConcurrentHashMap<Long,byte[]>requestIdToOrigMessageContents){
            this.workResultsQueue=workResultsQueue;
            this.workRequestsQueue=workRequestsQueue;
            this.myTCPAddress=myTCPAddress;
            this.zkpsi=zkpsi;
            this.requestIdToOrigMessageContents=requestIdToOrigMessageContents;
            try {
                this.logger = initializeLogging("logs/RRL-Log/result-listener-logs",ResultListenerThread.class.getCanonicalName() + "-on-RRL-on-TCP-port-"+this.myTCPAddress.getPort());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        public void shutdown(){
            interrupt();
            this.zkpsi=null;
            this.requestIdToOrigMessageContents=null;
            this.workRequestsQueue=null;
            this.workResultsQueue=null;
        }
        @Override
        public void run(){
            while(!isInterrupted()){
                try {
                    WorkResult wr=this.workResultsQueue.take();
                    long requestID=wr.getRequestId();
                    long workerID=wr.getWorkerId();
                    Socket socketToGateway=wr.getSocketToGateway();
                    if(wr.getCompletedExceptionally()||this.zkpsi.isPeerDead(workerID)){
                        this.logger.log(Level.FINE,"have to turn result back into request.  Result was: "+wr.toString());
                        WorkRequest newRequest=new WorkRequest(this.requestIdToOrigMessageContents.get(requestID),socketToGateway,requestID);
                        this.workRequestsQueue.put(newRequest);
                    }
                    else{
                        //send the message back to gateway
                        byte[] msgContents=wr.getResultMsgContents();
                        Message completedWork=new Message(Message.MessageType.COMPLETED_WORK,msgContents,this.myTCPAddress.getHostString(), this.myTCPAddress.getPort(), socketToGateway.getInetAddress().getHostName(),socketToGateway.getPort(),requestID);
                        socketToGateway.getOutputStream().write(completedWork.getNetworkPayload());
                        this.logger.log(Level.FINEST,"sent result of work to the gateway server: "+completedWork.toString());
                        while(true){
                            try{
                                socketToGateway.close();
                                break;
                            } catch(IOException e){
                                this.logger.log(Level.WARNING,"io exception while trying to close socket.  will retry");
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    this.logger.log(Level.WARNING,"Interrupted while blocking");
                    interrupt();
                } catch (IOException e) {
                    this.logger.log(Level.WARNING,"IOException while attempting to write: "+e.toString());
                }
            }
            this.logger.severe("WorkResultListener thread shutting down");
        }
    }
    class RequestListenerThread extends Thread implements LoggingServer{
        private InetSocketAddress myTCPAddress;
        private ThreadPoolExecutor tpe;
        private Logger logger;
        private volatile ZooKeeperPeerServerImpl zkpsi;
        private LinkedBlockingQueue<WorkResult> workResultsQueue;
        private LinkedBlockingQueue<WorkRequest> workRequestsQueue;
        public RequestListenerThread(InetSocketAddress myTCPAddress, ZooKeeperPeerServerImpl zkpsi, LinkedBlockingQueue<WorkRequest> workRequestsQueue,LinkedBlockingQueue<WorkResult> workResultsQueue){
            this.zkpsi=zkpsi;
            this.myTCPAddress=myTCPAddress;
            try {
                this.logger = initializeLogging("logs/RRL-Log/request-listener-logs",ResultListenerThread.class.getCanonicalName() + "-on-RRL-server-on-TCP-Port-" + this.myTCPAddress.getPort());
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.tpe= (ThreadPoolExecutor) Executors.newFixedThreadPool(zkpsi.getUnmodifiableViewOfLiveWorkers().size());
            this.workResultsQueue=workResultsQueue;
            this.workRequestsQueue=workRequestsQueue;
        }
        public void shutdown() {
            this.logger.log(Level.SEVERE,"Request Listener shutting down");
            this.interrupt();
            this.tpe.shutdown();
            this.zkpsi=null;
            this.workRequestsQueue=null;
            this.workResultsQueue=null;
        }
        @Override
        public void run(){
            this.logger.log(Level.FINE,"entering while loop in run");
            while(!Thread.currentThread().isInterrupted()){
                try {
                    this.logger.log(Level.FINE,"trying to take from queue");
                    WorkRequest wr=this.workRequestsQueue.take();
                    this.logger.log(Level.FINE,"took wr: "+wr.toString());
                    long requestId=wr.getRequestID();
                    byte[] requestContents=wr.getMessageContents();
                    Socket socketToGateway=wr.getSocketFromGateway();
                    WorkerNodeTCP next=this.zkpsi.getNextWorker();
                    this.logger.log(Level.FINE,"next Server up port: "+next.getTCPAddress().getPort());
                    this.logger.log(Level.FINE,"id of next Server up: "+next.getId());
                    //make sure the server didn't die in between the last two calls, resulting in an id that doesn't match the address
                    this.tpe.submit(new GetWorkDone(socketToGateway, next.getTCPAddress(),this.myTCPAddress,requestId,this.workResultsQueue,next.getId(),requestContents));
                } catch (InterruptedException e) {
                    this.logger.log(Level.WARNING,"interrupted while waiting for next request. Shutting down");
                    interrupt();
                }
            }
            this.logger.log(Level.SEVERE,"RequestListenerThread shutting down");
        }
    }
    class GetWorkDone implements Runnable{
        private Logger logger;
        private InetSocketAddress myWorker;
        private Socket socketToWorker;
        private long requestID;
        private InetSocketAddress myTCPAddress;
        private LinkedBlockingQueue<WorkResult> workResultsQueue;
        private long workerId;
        private Socket socketToGateway;
        private byte[] requestContents;
        public GetWorkDone(Socket socketToGateway, InetSocketAddress myWorker, InetSocketAddress myTCPAddress, long requestID, LinkedBlockingQueue<WorkResult> workResultsQueue, long workerId, byte[] requestContents){
            this.myWorker=myWorker;
            this.workResultsQueue=workResultsQueue;
            this.workerId=workerId;
            this.requestContents=requestContents;
            try {
                this.logger = initializeLogging("logs/Work-Logs",GetWorkDone.class.getCanonicalName() + "-using-worker-on-Port-" + this.myWorker.getPort());
            } catch (IOException e) {
                WorkResult wr=new WorkResult(null,true,e,workerId,requestID,socketToGateway);
                System.out.println("failed to create logger and got exception: "+"\nadding new Work Request to queue to reflect that: \n"+wr.toString());
                boolean done = false;
                while(!done){
                    done = this.workResultsQueue.offer(wr);
                }
            }
            this.socketToGateway=socketToGateway;
            this.myTCPAddress=myTCPAddress;
            this.requestID=requestID;
            try {
                this.socketToWorker=new Socket(myWorker.getHostName(),myWorker.getPort());
            } catch (IOException e) {
                WorkResult wr=new WorkResult(null,true,e,workerId,requestID,socketToGateway);
                this.logger.log(Level.FINE,"failed to create socket to worker, perhaps because it died and got exception"+"\nadding new Work Request to queue to reflect that\n"+wr.toString());

                boolean done = false;
                while(!done){
                    done = this.workResultsQueue.offer(wr);
                }
            }
        }

        @Override
        public void run() {
            try (InputStream inputFromWorker=this.socketToWorker.getInputStream();
                 OutputStream outputToWorker=this.socketToWorker.getOutputStream()){
                //sending that to the worker
                outputToWorker.write(new Message(Message.MessageType.WORK, this.requestContents, this.myTCPAddress.getHostString(), this.myTCPAddress.getPort(), myWorker.getHostString(), myWorker.getPort(),requestID).getNetworkPayload());
                this.logger.log(Level.FINEST,"sent output to the worker");
                //receive the response from the worker
                Message completedMessage= new Message(Util.readAllBytesFromNetwork(inputFromWorker));
                this.logger.log(Level.FINEST,"received the response from the worker: \n"+completedMessage.toString());

                //add to queue
                WorkResult wr= new WorkResult(completedMessage.getMessageContents(),false,null,workerId,requestID,socketToGateway);
                boolean done = false;
                while(!done){
                    done = this.workResultsQueue.offer(wr);
                }
                this.logger.log(Level.FINEST,"added result of work to the queue:\n"+wr.toString());
            } catch (IOException e) {
                WorkResult wr= new WorkResult(null,true,e,workerId,requestID,socketToGateway);
                this.logger.log(Level.WARNING, "hit IOException, adding result to queue "+wr.toString());
                boolean done = false;
                while(!done){
                    done = this.workResultsQueue.offer(wr);
                }
            }
            finally{
                while(true){
                    try {
                        this.socketToWorker.close();
                        this.logger.log(Level.FINEST,"closed socket to worker");
                        break;
                    } catch (IOException e) {
                        this.logger.log(Level.WARNING,"failed to close socket to worker, retrying");
                    }
                }
            }
        }
    }


    private ServerSocket myServerSocket;
    private InetSocketAddress myTCPAddress;
    private Logger logger;
    private volatile ZooKeeperPeerServerImpl zkpsi;
    private LinkedBlockingQueue<WorkResult> workResultsQueue;
    private LinkedBlockingQueue<WorkRequest> workRequestsQueue;
    private ConcurrentHashMap<Long,byte[]> requestIdToOrigContents;
    private ResultListenerThread resultLT;
    private RequestListenerThread requestLT;
    private boolean shutdown=false;
    private ConcurrentHashMap<Long,byte[]> requestIdsToCompletedWork;
    public RoundRobinLeader(InetSocketAddress myTCPAddress, ZooKeeperPeerServerImpl zkpsi, ConcurrentHashMap<Long,byte[]>requestIdToCompletedWork){
        this.requestIdsToCompletedWork=requestIdToCompletedWork;
        this.zkpsi=zkpsi;
        this.myTCPAddress=myTCPAddress;
        this.workRequestsQueue=new LinkedBlockingQueue<>();
        this.workResultsQueue=new LinkedBlockingQueue<>();
        this.requestIdToOrigContents=new ConcurrentHashMap<>();
        this.requestLT=new RequestListenerThread(myTCPAddress,zkpsi,workRequestsQueue,workResultsQueue);
        this.requestLT.setName("request-listener-thread-for-rrl-on-tcp-port-"+this.myTCPAddress.getPort());
        this.requestLT.setDaemon(true);
        this.resultLT=new ResultListenerThread(workResultsQueue,workRequestsQueue,myTCPAddress,zkpsi,requestIdToOrigContents);
        this.resultLT.setName("result-listener-thread-for-rrl-on-tcp-port-"+this.myTCPAddress.getPort());
        this.resultLT.setDaemon(true);
        try {
            this.logger = initializeLogging("logs/RRL-Log",RoundRobinLeader.class.getCanonicalName() + "-on-Leader-server-on-Port-" + this.myTCPAddress.getPort());
        } catch (IOException e) {
            e.printStackTrace();
        }
        while(true) {
            try {
                this.myServerSocket = new ServerSocket(myTCPAddress.getPort());
                break;
            } catch (IOException e) {
                this.logger.warning("hit IO exception when trying to create ServerSocket.\n"+e.toString());
                e.printStackTrace();
            }
        }
        //get all completed work that my workers stored
        for(InetSocketAddress worker: this.zkpsi.getUnmodifiableViewOfLiveWorkers()){
            try (Socket socketToWorker=new Socket(worker.getHostName(),worker.getPort());
                 InputStream inputFromWorker=socketToWorker.getInputStream();
                 OutputStream outputToWorker=socketToWorker.getOutputStream()){
                //sending that to the worker
                outputToWorker.write(new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK,  "Please send your completed work".getBytes(StandardCharsets.UTF_8), this.myTCPAddress.getHostString(), this.myTCPAddress.getPort(), worker.getHostString(), worker.getPort()).getNetworkPayload());
                this.logger.log(Level.FINEST,"sent output to the worker to get last work");
                //receive the response from the worker
                Message completedMessage= new Message(Util.readAllBytesFromNetwork(inputFromWorker));
                while(!new String(completedMessage.getMessageContents()).equals("DONE")){
                    this.logger.log(Level.FINEST,"received the response from the worker: \n"+completedMessage.toString()+"\n adding to my hashmap");
                    this.requestIdsToCompletedWork.put(completedMessage.getRequestID(),completedMessage.getMessageContents());
                }
            } catch (IOException e){
                this.logger.log(Level.WARNING, "problem connecting to worker on address: "+worker.getPort()+" to get completed work stored");
            }
        }
    }
    public void shutdown() {
        this.logger.log(Level.SEVERE,"RRL shutting down");
        shutdown=true;
        Thread.currentThread().interrupt();
        try {
            this.myServerSocket.close();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE,"rrl can't shut down server socket",e);
        }
        this.resultLT.shutdown();
        this.requestLT.shutdown();
    }
    @Override
    public void run(){
        this.requestLT.start();
        this.resultLT.start();
        while(!shutdown){
            this.logger.log(Level.FINEST, "\nstarting run while loop\n status of interrupt: "+Thread.currentThread().isInterrupted()+"\nstatus of shutdown: "+shutdown+"\n");
            InputStream is=null;
            try {
                Socket next=myServerSocket.accept();
                is=next.getInputStream();
                Message m=new Message(Util.readAllBytesFromNetwork(is));
                this.logger.log(Level.FINEST,"received the work to be done: \n"+m.toString());
                long requestID=m.getRequestID();
                byte[] workContents=m.getMessageContents();
                this.requestIdToOrigContents.put(requestID,workContents);
                if(this.requestIdsToCompletedWork.contains(requestID)){
                    this.logger.log(Level.FINE,"completed work already contains id: "+requestID+" with result: "+this.requestIdsToCompletedWork.get(requestID)+" so retrieving and sending result");
                    Message completedWork=new Message(Message.MessageType.COMPLETED_WORK,this.requestIdsToCompletedWork.get(requestID),this.myTCPAddress.getHostString(), this.myTCPAddress.getPort(), next.getInetAddress().getHostName(),next.getPort(),requestID);
                    next.getOutputStream().write(completedWork.getNetworkPayload());
                }
                else{
                    this.logger.log(Level.FINE,"no existing record for that work, creating new request for it");
                    WorkRequest wr=new WorkRequest(m.getMessageContents(),next,requestID);
                    this.logger.log(Level.FINE,"put work request representing work to do on the queue: "+wr.toString());
                    this.workRequestsQueue.put(wr);
                }
            } catch (IOException e) {
                this.logger.log(Level.WARNING,"IoException while trying to accept next socket, or get input stream: "+e.toString());
            } catch (InterruptedException e) {
                this.logger.log(Level.WARNING, "Interrupted during put");
            }
        }
        this.logger.log(Level.SEVERE,"Exiting RRL.run()");
    }
}