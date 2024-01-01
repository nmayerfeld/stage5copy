package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;

public class ZookeeperPeerServerImplTest2 {
    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    Set<String> expectedResults=new HashSet<>();
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private int[] ports = {8010, 8020, 8030, 8040,8050,8060,8070,8080};
    //private int[] ports = {8010, 8020};
    private int myPort = 9999;
    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private ArrayList<ZooKeeperPeerServer> servers;
    private List<CompletableFuture<HttpResponse<String>>> responses=new ArrayList<>();
    private GatewayServer gs;
    public void leaderKilledTest() throws InterruptedException, ExecutionException {
        //step 1: create sender & sending queue
        //step 2: create servers
        try {
            createServers();
        } catch (IOException e) {
            e.printStackTrace();
        }
        HttpClient client=HttpClient.newBuilder().build();
        URL url= null;
        try {
            url = new URL("http://localhost:8090/compileandrun");
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        //step2.1: wait for servers to get started
        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
        }
        printLeaders();
        //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
        for (int i = 0; i < 30; i++) {
            String code = this.validClass.replace("world!", "world! from code version " + i);
            expectedResults.add("Hello world! from code version "+i+"   200");
            sendMessage(client,code,url);
        }
        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException e) {
            Thread.sleep(3000);
        }
        System.out.println("killing worker at "+System.currentTimeMillis());
        this.servers.get(7).shutdown();
        System.out.println("finished killing worker at "+System.currentTimeMillis());
        try {
            Thread.sleep(30000);
        }
        catch (InterruptedException e) {
            Thread.sleep(30000);
        }
        this.servers.get(0).shutdown();
        System.out.println("finished killing worker at "+System.currentTimeMillis());
        try {
            Thread.sleep(60000);
        }
        catch (InterruptedException e) {
            Thread.sleep(60000);
        }
        for (int i = 30; i < 60; i++) {
            String code = this.validClass.replace("world!", "world! from code version " + i);
            expectedResults.add("Hello world! from code version "+i+"   200");
            sendMessage(client,code,url);
        }
        //step 4: validate responses from leader
        for(CompletableFuture<HttpResponse<String>> r:this.responses){
            try{
                Thread.sleep(2000);
            }catch(InterruptedException e){
                Thread.sleep(2000);
            }
            HttpResponse<String> res=r.get();
            assertEquals(res.statusCode(),200);
            if(!this.expectedResults.contains(res.body())){
                assert false;
            }
        }
        //step 5: stop servers
        stopServers();
    }
    private void printLeaders() {
        for (ZooKeeperPeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
        }
    }

    private void stopServers() {
        for (int i=1;i<this.servers.size()-1;i++) {
            this.servers.get(i).shutdown();
        }
        this.gs.stop();
    }


    private void sendMessage(HttpClient client,String code, URL url) throws InterruptedException {
        HttpRequest request=null;
        try {
            request = HttpRequest
                    .newBuilder(url.toURI())
                    .headers("Content-Type","text/x-java-source")
                    .POST(HttpRequest.BodyPublishers.ofString(code))
                    .build();
        } catch (URISyntaxException e) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            e.printStackTrace(printStream);
            assert false;
        }
        CompletableFuture<HttpResponse<String>> requestResponse = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        this.responses.add(requestResponse);
    }

    private void createServers() throws IOException {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < this.ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
        }
        //create servers
        this.servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server;
            if(entry.getKey()!=1){
                server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map,1);
            }
            else{
                server=new GatewayPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
                gs=new GatewayServer(8090,(GatewayPeerServerImpl) server);
                gs.start();
            }
            this.servers.add(server);
            server.start();
        }
    }
}
