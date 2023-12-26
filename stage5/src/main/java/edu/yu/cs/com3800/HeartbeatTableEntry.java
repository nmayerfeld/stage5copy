package edu.yu.cs.com3800;

public class HeartbeatTableEntry {
    private long heartbeatCounter;
    private long localTime;
    private boolean failed;
    private long serverId;
    public HeartbeatTableEntry(long serverId,long heartbeatCounter, long localTime,boolean failed){
        this.serverId=serverId;
        this.heartbeatCounter=heartbeatCounter;
        this.localTime=localTime;
        this.failed=failed;
    }
    public long getHeartbeatCounter(){return this.heartbeatCounter;}
    public long getLocalTime(){return this.localTime;}
    public boolean getFailed(){return this.failed;}
    public long getServerId(){return this.serverId;}
    public boolean shouldBeReplacedBy(HeartbeatTableEntry received, long serverIdReceivedFrom){
        //if this has node__ marked as failed but entry was received from Node__, replace
        //deprecated- don't use this for this stage even though ideal system would listen to a direct heartbeat after marked as gailed
        // if(this.failed&&this.serverId==serverIdReceivedFrom){return true;}
        //if either has failed set, or dealing with different entries representing different nodes, don't replace
        if(this.failed||received.getFailed()||this.serverId!=received.getServerId()){
            return false;
        }
        //if this heartbeat counter> old heartbeatcounter, replace
        if(received.getHeartbeatCounter()>this.heartbeatCounter){
            return true;
        }
        return false;
    }
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HeartbeatTableEntry)) {
            return false;
        }
        HeartbeatTableEntry other = (HeartbeatTableEntry) o;
        if(this.serverId==other.getServerId()&&this.heartbeatCounter== other.getHeartbeatCounter()&&this.failed==other.getFailed()){
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(serverId)*Long.hashCode(heartbeatCounter)*Long.hashCode(localTime);
    }

    public String toString() {
        return "SERVER: "+serverId+"\nHEARTBEAT COUNTER: "+heartbeatCounter+"\nTIME: "+localTime+"\nFailed: "+failed;
    }

}
