package edu.yu.cs.com3800.stage5;

import org.junit.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class ZooKeeperPeerServerImplTest {
    private static List<CompletableFuture<HttpResponse<String>>> responses=new ArrayList<>();
    private static String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";

    @Test
    public void simpleTest(){
        String filePath="output.txt";
        File file = new File(filePath);
        HttpClient client=HttpClient.newBuilder().build();
        URL urlForRequests;
        URL urlForCheckingLeader;
        try {
            urlForRequests=new URL("http://localhost:8090/compileandrun");
            urlForCheckingLeader=new URL("http://localhost:8090/hasleader");
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return;
        }

        int [] ports={8010,8020,8030,8040,8050,8060,8070,8080};
        String representationOfMap="0:localhost:8010\n1:localhost:8020\n2:localhost:8030\n3:localhost:8040\n4:localhost:8050\n5:localhost:8060\n6:localhost:8070\n7:localhost:8080";
        long gatewayID=1;
        List<Process> myProcesses=new ArrayList<>();
        for(int i=0;i<8;i++){
            ProcessBuilder processBuilder = new ProcessBuilder(
                    "java",
                    "-cp",
                    "target/classes",
                    "edu.yu.cs.com3800.stage5.ZKPSInstance",
                    String.valueOf(ports[i]),
                    String.valueOf(0),
                    representationOfMap,
                    String.valueOf(gatewayID),
                    String.valueOf((long)(i))
            );
            processBuilder.inheritIO();
            try {
                myProcesses.add(processBuilder.start());
            } catch (IOException e) {
                e.printStackTrace();
                for(int j=0;j<myProcesses.size();j++){
                    myProcesses.get(i).destroy();
                }
                assert false;
                return;
            }
        }
        HttpRequest request=null;
        HttpResponse<String> requestResponse=null;
        try {
            request = HttpRequest
                    .newBuilder(urlForCheckingLeader.toURI())
                    .headers("Content-Type","text/x-java-source")
                    .GET()
                    .build();
            requestResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (URISyntaxException | IOException | InterruptedException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            e.printStackTrace(printStream);
            for(int i=0;i<myProcesses.size();i++){
                myProcesses.get(i).destroy();
            }
            assert false;
            return;
        }
        while(requestResponse==null||requestResponse.statusCode()==425){
            try {
                Thread.sleep(1000);
                request = HttpRequest
                        .newBuilder(urlForCheckingLeader.toURI())
                        .headers("Content-Type","text/x-java-source")
                        .GET()
                        .build();
                requestResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (URISyntaxException |IOException|InterruptedException e) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                PrintStream printStream = new PrintStream(outputStream);
                e.printStackTrace(printStream);
                for(int i=0;i<myProcesses.size();i++){
                    myProcesses.get(i).destroy();
                }
                assert false;
                return;
            }
        }
        assert requestResponse.statusCode()==200;
        System.out.println(requestResponse.body());
        try (FileWriter fileWriter = new FileWriter(file)) {
            // Write the content to the file
            fileWriter.write(requestResponse.body());
        } catch (IOException e) {
            // Handle exceptions, such as file not found or permission issues
            System.err.println("Error writing to the file: " + e.getMessage());
            assert false;
        }
        //ADD TO THE OUTPUT FILE

        //send 9 requests
        for (int i = 0; i < 9; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            try {
                sendMessage(client,code,urlForRequests);
            } catch (InterruptedException e) {
                e.printStackTrace();
                for(int j=0;j<myProcesses.size();j++){
                    myProcesses.get(i).destroy();
                }
                assert false;
                return;
            }
        }
        for(CompletableFuture<HttpResponse<String>> r:List.copyOf(responses)){
            try {
                Thread.sleep(5000);
                HttpResponse<String> res=r.get();
                if(res.statusCode()==200) {
                    System.out.println(r.get().body() + "   " + r.get().statusCode());
                    try (FileWriter fileWriter = new FileWriter(file)) {
                        // Write the content to the file
                        fileWriter.write(r.get().body() + "   " + r.get().statusCode());
                    } catch (IOException e) {
                        // Handle exceptions, such as file not found or permission issues
                        System.err.println("Error writing to the file: " + e.getMessage());
                        assert false;
                    }
                    System.out.println(r.get().body() + "   " + r.get().statusCode());
                }
                else{
                    try (FileWriter fileWriter = new FileWriter(file)) {
                        // Write the content to the file
                        fileWriter.write("Error.  Status code is: "+res.statusCode()+" with error: "+res.toString());
                    } catch (IOException e) {
                        // Handle exceptions, such as file not found or permission issues
                        System.err.println("Error writing to the file: " + e.getMessage());
                        assert false;
                    }
                    System.out.println("Error.  Status code is: "+res.statusCode()+" with error: "+res.toString());
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                for(int i=0;i<myProcesses.size();i++){
                    myProcesses.get(i).destroy();
                }
                assert false;
                return;
            }
        }
        responses.clear();

        try (FileWriter fileWriter = new FileWriter(file)) {
            // Write the content to the file
            fileWriter.write("Killing worker server with ID: 1");
        } catch (IOException e) {
            // Handle exceptions, such as file not found or permission issues
            System.err.println("Error writing to the file: " + e.getMessage());
            assert false;
        }
        System.out.println("Killing worker " +"server with ID: 1");
        myProcesses.get(0).destroy();
        //sleep
        try {
            Thread.sleep(3000*15);
        } catch (InterruptedException e) {
            e.printStackTrace();
            for(int i=0;i<myProcesses.size();i++){
                myProcesses.get(i).destroy();
            }
            assert false;
            return;
        }
        try {
            request = HttpRequest
                    .newBuilder(urlForCheckingLeader.toURI())
                    .headers("Content-Type","text/x-java-source")
                    .GET()
                    .build();
            requestResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
            assert requestResponse.statusCode()==200;
            System.out.println(requestResponse.body());
            try (FileWriter fileWriter = new FileWriter(file)) {
                // Write the content to the file
                fileWriter.write(requestResponse.body());
            } catch (IOException e) {
                // Handle exceptions, such as file not found or permission issues
                System.err.println("Error writing to the file: " + e.getMessage());
                assert false;
            }
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
            for(int i=0;i<myProcesses.size();i++){
                myProcesses.get(i).destroy();
            }
            assert false;
            return;
        }

        //kill leader
        myProcesses.get(7).destroy();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            for(int i=0;i<myProcesses.size();i++){
                myProcesses.get(i).destroy();
            }
            assert false;
            return;
        }

        //send 9 requests
        for (int i = 9; i < 18; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            try {
                sendMessage(client,code,urlForRequests);
            } catch (InterruptedException e) {
                e.printStackTrace();
                for(int j=0;j<myProcesses.size();j++){
                    myProcesses.get(i).destroy();
                }
                assert false;
            }
        }



        //wait for a new leader to be elected
        try {
            request = HttpRequest
                    .newBuilder(urlForCheckingLeader.toURI())
                    .headers("Content-Type","text/x-java-source")
                    .GET()
                    .build();
            requestResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (URISyntaxException | IOException | InterruptedException e) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(outputStream);
            e.printStackTrace(printStream);
            for(int i=0;i<myProcesses.size();i++){
                myProcesses.get(i).destroy();
            }
            assert false;
            return;
        }
        while(requestResponse==null||requestResponse.statusCode()==425){
            try {
                Thread.sleep(1000);
                request = HttpRequest
                        .newBuilder(urlForCheckingLeader.toURI())
                        .headers("Content-Type","text/x-java-source")
                        .GET()
                        .build();
                requestResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (URISyntaxException | IOException | InterruptedException e) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                PrintStream printStream = new PrintStream(outputStream);
                e.printStackTrace(printStream);
                for(int i=0;i<myProcesses.size();i++){
                    myProcesses.get(i).destroy();
                }
                assert false;
                return;
            }
        }
        assert requestResponse.statusCode()==200;
        String result=requestResponse.body();
        String [] entries=result.split("\n");
        for(String entry:entries){
            String []split=entry.split(":");
            if(split[2].equals("LEADER")){
                System.out.println("New Leader ID: "+split[1]);
                try (FileWriter fileWriter = new FileWriter(file)) {
                    // Write the content to the file
                    fileWriter.write("New Leader ID: "+split[1]);
                } catch (IOException e) {
                    // Handle exceptions, such as file not found or permission issues
                    System.err.println("Error writing to the file: " + e.getMessage());
                    assert false;
                }
            }
        }



        for(CompletableFuture<HttpResponse<String>> r:List.copyOf(responses)){
            try {
                Thread.sleep(5000);
                HttpResponse<String> res=r.get();
                if(res.statusCode()==200) {
                    System.out.println(r.get().body() + "   " + r.get().statusCode());
                    try (FileWriter fileWriter = new FileWriter(file)) {
                        // Write the content to the file
                        fileWriter.write(r.get().body() + "   " + r.get().statusCode());
                    } catch (IOException e) {
                        // Handle exceptions, such as file not found or permission issues
                        System.err.println("Error writing to the file: " + e.getMessage());
                        assert false;
                    }

                    //ADD TO OUTPUT LOG
                }
                else{
                    System.out.println("Error.  Status code is: "+res.statusCode()+" with error: "+res.toString());
                    try (FileWriter fileWriter = new FileWriter(file)) {
                        // Write the content to the file
                        fileWriter.write("Error.  Status code is: "+res.statusCode()+" with error: "+res.toString());
                    } catch (IOException e) {
                        // Handle exceptions, such as file not found or permission issues
                        System.err.println("Error writing to the file: " + e.getMessage());
                        assert false;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                for(int i=0;i<myProcesses.size();i++){
                    myProcesses.get(i).destroy();
                }
                assert false;
                return;
            }
        }
        responses.clear();




        //send final request
        try {
            request = HttpRequest
                    .newBuilder(urlForCheckingLeader.toURI())
                    .headers("Content-Type","text/x-java-source")
                    .GET()
                    .build();
            requestResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
            assert requestResponse.statusCode()==200;
            System.out.println("Response was: "+requestResponse.body());
            try (FileWriter fileWriter = new FileWriter(file)) {
                // Write the content to the file
                fileWriter.write("response was: "+requestResponse.body());
            } catch (IOException e) {
                // Handle exceptions, such as file not found or permission issues
                System.err.println("Error writing to the file: " + e.getMessage());
                assert false;
            }
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
            for(int i=0;i<myProcesses.size();i++){
                myProcesses.get(i).destroy();
            }
            assert false;
            return;
        }
        for(int i=0;i<myProcesses.size();i++){
            myProcesses.get(i).destroy();
        }

    }
    private static void sendMessage(HttpClient client,String code, URL url) throws InterruptedException {
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
}