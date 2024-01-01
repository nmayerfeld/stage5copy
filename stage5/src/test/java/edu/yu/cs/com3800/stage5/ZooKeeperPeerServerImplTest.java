package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
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
import static org.junit.jupiter.api.Assertions.*;

public class ZooKeeperPeerServerImplTest {
    @Test
    public void leaderKilledTest() throws InterruptedException, ExecutionException {
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        Set<String> expectedResults=new HashSet<>();
        LinkedBlockingQueue<Message> outgoingMessages;
        LinkedBlockingQueue<Message> incomingMessages;
        int[] ports = {8010, 8020, 8030, 8040,8050,8060,8070,8080};
        //private int[] ports = {8010, 8020};
        int myPort = 9999;
        InetSocketAddress myAddress = new InetSocketAddress("localhost", myPort);
        ArrayList<ZooKeeperPeerServer> servers;
        List<CompletableFuture<HttpResponse<String>>> responses=new ArrayList<>();
        GatewayServer gs=null;
        //step 1: create sender & sending queue
        //step 2: create servers
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", ports[i]));
        }
        //create servers
        servers = new ArrayList<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server;
            if (entry.getKey() != 1) {
                server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 1);
            } else {
                server = new GatewayPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
                gs = new GatewayServer(8090, (GatewayPeerServerImpl) server);
                gs.start();
            }
            servers.add(server);
            server.start();
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
        //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
        for (int i = 0; i < 30; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            expectedResults.add("Hello world! from code version "+i+"   200");
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
            responses.add(requestResponse);
        }
        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException e) {
            Thread.sleep(3000);
        }
        System.out.println("killing worker at "+System.currentTimeMillis());
        servers.get(7).shutdown();
        System.out.println("finished killing worker at "+System.currentTimeMillis());
        try {
            Thread.sleep(30000);
        }
        catch (InterruptedException e) {
            Thread.sleep(30000);
        }
        servers.get(0).shutdown();
        System.out.println("finished killing worker at "+System.currentTimeMillis());
        try {
            Thread.sleep(60000);
        }
        catch (InterruptedException e) {
            Thread.sleep(60000);
        }
        for (int i = 30; i < 60; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            expectedResults.add("Hello world! from code version "+i+"   200");
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
            responses.add(requestResponse);
        }
        //step 4: validate responses from leader
        for(CompletableFuture<HttpResponse<String>> r:responses){
            try{
                Thread.sleep(2000);
            }catch(InterruptedException e){
                Thread.sleep(2000);
            }
            HttpResponse<String> res=r.get();
            assertEquals(res.statusCode(),200);
            if(!expectedResults.contains(res.body())){
                System.out.println(res.body());
                assert false;
            }
        }
        //step 5: stop servers
        for (int i=1;i<servers.size()-1;i++) {
            servers.get(i).shutdown();
        }
        if(gs!=null){
            gs.stop();
        }
    }

}