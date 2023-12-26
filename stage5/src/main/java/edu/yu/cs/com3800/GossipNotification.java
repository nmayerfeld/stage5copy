package edu.yu.cs.com3800;

import java.util.Map;

public class GossipNotification {
    private Map<Long, HeartbeatTableEntry> htEntries;
    private long serverIDofSender;
    public GossipNotification(Map<Long,HeartbeatTableEntry> htEntries, long serverIDofSender){
        this.htEntries=htEntries;
        this.serverIDofSender=serverIDofSender;
    }
    public Map<Long,HeartbeatTableEntry> getHtEntries(){return this.htEntries;}
    public long getServerIDofSender(){return this.serverIDofSender;}
    @Override
    public String toString(){
        String result="Gossip Notification\nFrom Server: "+this.serverIDofSender+"\n with entries";
        for(HeartbeatTableEntry hte:this.htEntries.values()){
            result=result+"\n"+hte.toString();
        }
        return result;
    }

}
