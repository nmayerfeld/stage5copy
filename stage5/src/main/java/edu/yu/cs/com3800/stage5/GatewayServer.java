package edu.yu.cs.com3800.stage5;


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayServer implements LoggingServer {
    class RequestToGateway{
        private HttpExchange exchange;
        private byte[] requestContents;
        public RequestToGateway(HttpExchange exchange, byte[] requestContents){
            this.exchange=exchange;
            this.requestContents=requestContents;
        }
        public HttpExchange getExchange(){return this.exchange;}
        public byte[] getRequestContents(){return this.requestContents;}
        @Override
        public String toString(){
            return "Request to gateway with contents: "+new String(this.requestContents);
        }
    }
    class RequestHandlerThread extends Thread{
        private LinkedBlockingQueue<RequestToGateway> waitingRequests;
        private volatile GatewayPeerServerImpl gpsi;
        private Logger l;
        InetSocketAddress myTCPAddress;
        private volatile AtomicLong requestID=new AtomicLong(0);
        public RequestHandlerThread(LinkedBlockingQueue<RequestToGateway> waitingRequests, GatewayPeerServerImpl gpsi, InetSocketAddress myTCPAddress){
            this.waitingRequests=waitingRequests;
            this.gpsi=gpsi;
            try {
                this.l=initializeLogging("logs/Gateway-Log/RequestHandlerThread/", RequestHandlerThread.class.getCanonicalName() + "-on-Gateway");
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.myTCPAddress=myTCPAddress;
        }
        public void shutdown(){
            interrupt();
            this.waitingRequests=null;
            this.gpsi=null;
        }
        @Override
        public void run(){
            while(!isInterrupted()){
                try {
                    RequestToGateway rtg=this.waitingRequests.take();
                    this.l.log(Level.FINE,"RequestHandlerThread took request off queue: "+rtg);
                    HttpExchange t= rtg.getExchange();
                    byte[] b=rtg.getRequestContents();
                    //if leader is dead and currently looking for new one;
                    if(this.gpsi.getCurrentLeader()==null||this.gpsi.getPeerByID(this.gpsi.getCurrentLeader().getProposedLeaderID())==null){
                        RequestToGateway req=new RequestToGateway(t,b);
                        this.l.log(Level.FINE,"leader is currently dead and must be looking for new one, so putting request on a queue: "+req.toString());
                        this.waitingRequests.put(req);
                        Thread.sleep(10000);
                    }
                    else {
                        try{
                            this.l.log(Level.FINE,"getting id of leader to send the request to: "+this.gpsi.getCurrentLeader().getProposedLeaderID()+"\n with address: "+this.gpsi.getTCPPeerByID(this.gpsi.getCurrentLeader().getProposedLeaderID()));
                            InetSocketAddress TCPAddressOfLeader=this.gpsi.getTCPPeerByID(this.gpsi.getCurrentLeader().getProposedLeaderID());
                            if(TCPAddressOfLeader==null){
                                RequestToGateway req=new RequestToGateway(t,b);
                                this.l.log(Level.FINE,"leader is currently null and must be looking for new one, so putting request on a queue: "+req.toString());
                                this.waitingRequests.put(req);
                                Thread.sleep(10000);
                                continue;
                            }
                            try(Socket socketToLeader=new Socket(TCPAddressOfLeader.getHostName(),TCPAddressOfLeader.getPort());
                                InputStream inputFromLeader = socketToLeader.getInputStream();
                                OutputStream outputToLeader = socketToLeader.getOutputStream()){
                                outputToLeader.write(new Message(Message.MessageType.WORK, b, this.myTCPAddress.getHostString(), this.myTCPAddress.getPort(), TCPAddressOfLeader.getHostName(), TCPAddressOfLeader.getPort(),this.requestID.getAndIncrement()).getNetworkPayload());
                                this.l.log(Level.FINEST,"sent output to the leader");
                                Message completedMessage= new Message(Util.readAllBytesFromNetwork(inputFromLeader));
                                this.l.log(Level.FINEST,"received the response from the Leader: \n"+completedMessage.toString());
                                if(this.gpsi.getCurrentLeader()==null||this.gpsi.getTCPPeerByID(this.gpsi.getCurrentLeader().getProposedLeaderID())==null){
                                    RequestToGateway request=new RequestToGateway(t,b);
                                    this.l.log(Level.FINE,"leader is now dead, so putting request on the queue and going to rerun: "+request.toString());
                                    this.waitingRequests.put(request);
                                    Thread.sleep(10000);
                                }
                                else{
                                    t.sendResponseHeaders(200, completedMessage.getMessageContents().length);
                                    OutputStream os = t.getResponseBody();
                                    os.write(completedMessage.getMessageContents());
                                    os.close();
                                    t.close();
                                }
                            } catch (Exception e) {
                                this.l.log(Level.FINE,"IOException so putting request back on queue: ",e);
                                RequestToGateway request=new RequestToGateway(t,b);
                                this.waitingRequests.put(request);
                            }
                        }catch (NullPointerException e){
                            this.l.log(Level.FINE,"got a NullPointerException somewhere, so putting request back on queue: ",e);
                            RequestToGateway request=new RequestToGateway(t,b);
                            this.waitingRequests.put(request);
                        }
                    }
                } catch (InterruptedException e) {
                    this.l.log(Level.FINE,"interrupted while waiting for request, shutting down RequestHandlerThread.run()");
                    interrupt();
                }

            }
        }
    }
    class PostHandler implements HttpHandler {
        private Logger l;
        private final InetSocketAddress myTCPAddress;
        private LinkedBlockingQueue<RequestToGateway> waitingRequests;
        public PostHandler(LinkedBlockingQueue<RequestToGateway> waitingRequests, Logger l, InetSocketAddress myTCPAddress){
            this.waitingRequests=waitingRequests;
            this.l=l;
            this.myTCPAddress=myTCPAddress;
        }
        public void handle(HttpExchange t) throws IOException {
            try {
                if(!t.getRequestHeaders().get("Content-Type").get(0).equals("text/x-java-source")){
                    this.l.fine("received a request with the wrong content type, response will include an error");
                    String response="content was not of correct type";
                    File f=new File("message.txt");
                    try {
                        Files.createFile(Path.of("message.txt"));
                        Files.writeString(Path.of("message.txt"),"type was "+t.getRequestHeaders().get("Content-Type").get(0));
                        this.l.fine("type was"+t.getRequestHeaders().get("Content-Type").get(0));
                        t.sendResponseHeaders(400,response.length());
                        OutputStream os = t.getResponseBody();
                        os.write(response.getBytes());
                        os.close();
                    } catch (IOException e) {
                        this.l.warning("IOException while sending back response that request had some errors in it");
                    }
                    finally {
                        t.close();
                    }
                }
                //if leader is dead and currently looking for new one;
                else {
                    byte[] b=t.getRequestBody().readAllBytes();
                    RequestToGateway rtg=new RequestToGateway(t,b);
                    this.waitingRequests.put(rtg);
                    this.l.log(Level.FINE,"PostHandler.handle is putting request on a queue: "+rtg.toString());
                }
            } catch (InterruptedException e) {
                this.l.log(Level.WARNING,"thread was interrupted before leader was set");
                Thread.currentThread().interrupt();
            }
        }
    }
    class GetHandler implements HttpHandler{
        private volatile GatewayPeerServerImpl gpsi;
        public GetHandler(GatewayPeerServerImpl gpsi){
            this.gpsi=gpsi;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Vote currentLeader=this.gpsi.getCurrentLeader();
            if(currentLeader!=null){
                String result="";
                for(long l: this.gpsi.getServerIDsOfAlivePeers()){
                    if(l==currentLeader.getProposedLeaderID()){
                        result+="Server: "+l+": "+"LEADER\n";
                    }
                    else if(l==this.gpsi.getGatewayID()){
                        result+="Server: "+l+": "+"OBSERVER\n";
                    }
                    else{
                        result+="Server: "+l+": "+"FOLLOWER\n";
                    }
                }
                exchange.sendResponseHeaders(200, result.length());
                OutputStream os = exchange.getResponseBody();
                os.write(result.getBytes(StandardCharsets.UTF_8));
                os.close();
                exchange.close();
            }
            else{
                String result="No Leader Chosen Yet";
                exchange.sendResponseHeaders(425, result.length());
                OutputStream os = exchange.getResponseBody();
                os.write(result.getBytes(StandardCharsets.UTF_8));
                os.close();
                exchange.close();
            }
        }
    }
    private int httpPort;
    private HttpServer server;
    private GatewayPeerServerImpl gpsi;
    private Logger logger;
    private InetSocketAddress myTCPAddress;
    private ThreadPoolExecutor tpe;
    private LinkedBlockingQueue<RequestToGateway> waitingRequests;
    private RequestHandlerThread rht;
    public GatewayServer(int httpPort, GatewayPeerServerImpl gatewayPeerServerImpl){
        this.httpPort=httpPort;
        this.gpsi= gatewayPeerServerImpl;
        this.waitingRequests=new LinkedBlockingQueue<>();
        this.myTCPAddress=new InetSocketAddress(this.gpsi.getAddress().getHostName(),this.gpsi.getAddress().getPort()+2);
        try{
            logger=initializeLogging("logs/Gateway-Log", GatewayServer.class.getCanonicalName() + "-using-worker-on-Port-" + this.httpPort);
        }catch (IOException e) {
            e.printStackTrace();
        }
        while(true){
            try {
                this.server = HttpServer.create(new InetSocketAddress(httpPort), 0);
                break;
            } catch (IOException e) {
                System.out.println("failed to create httpserver");
                this.logger.log(Level.FINE,"couldn't create HTTPServer, retrying");
            }
        }
        this.tpe = (ThreadPoolExecutor) Executors.newFixedThreadPool(12);
        server.createContext("/compileandrun", new PostHandler(this.waitingRequests,this.logger,new InetSocketAddress(this.myTCPAddress.getHostName(),this.myTCPAddress.getPort())));
        server.setExecutor(tpe);
        server.createContext("/hasleader", new GetHandler(this.gpsi));
        this.rht=new RequestHandlerThread(this.waitingRequests,this.gpsi,this.myTCPAddress);
    }
    public void start() {
        System.out.println("starting gateway server");
        this.server.start();
        rht.start();
    }
    public void stop() {
        tpe.shutdown();
        rht.shutdown();
        this.server.stop(3);
    }
}
