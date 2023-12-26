package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;

import java.io.OutputStream;
import java.net.Socket;

public class WorkRequest {
    private byte[] messageContents;
    private Socket socketFromGateway;
    private long requestID;
    public WorkRequest(byte[] messageContents, Socket socketFromGateway, long requestID){
        this.messageContents=messageContents;
        this.socketFromGateway=socketFromGateway;
        this.requestID=requestID;
    }
    public byte[] getMessageContents(){return this.messageContents;}
    public Socket getSocketFromGateway(){return this.socketFromGateway;}
    public long getRequestID(){return this.requestID;}
    @Override
    public String toString(){
        return "WorkRequest:\nRequestID: "+requestID+"\nMessage Contents: "+new String(messageContents)+"\nSocketFromGateway: "+socketFromGateway;
    }
}
