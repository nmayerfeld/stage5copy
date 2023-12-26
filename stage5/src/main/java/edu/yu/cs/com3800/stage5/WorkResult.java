package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.Message;

import java.net.Socket;

public class WorkResult {
    private byte[] resultMsgContents;
    private boolean completedExceptionally;
    private Exception e;
    private long workerId;
    private long requestId;
    private Socket socketToGateway;
    public WorkResult(byte[] resultMsgContents,boolean completedExceptionally, Exception e, long workerId, long requestID, Socket socketToGateway){
        this.resultMsgContents=resultMsgContents;
        this.completedExceptionally=completedExceptionally;
        this.e=e;
        this.workerId=workerId;
        this.requestId=requestID;
        this.socketToGateway=socketToGateway;
    }
    public byte[] getResultMsgContents(){return this.resultMsgContents;}
    public boolean getCompletedExceptionally(){return this.completedExceptionally;}
    public Exception getE(){return this.e;}
    public long getWorkerId(){return this.workerId;}
    public long getRequestId(){return this.requestId;}
    public Socket getSocketToGateway(){return this.socketToGateway;}
    @Override
    public String toString(){
        String result="Work Result:"+"\nRequestID: "+requestId+"\non worker: "+workerId;
        if(resultMsgContents!=null){
            result+="\nMessage:\n"+new String(resultMsgContents);
        }
        if(completedExceptionally){
            result+="\nException"+e.toString();
        }
        result+="\nSocket to gateway: "+socketToGateway;
        return result;
    }
}
