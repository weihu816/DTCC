package cn.ict.dtcc.config;

public class MemberId {

	int shardId;
	int procId;
	String id;
	
	public MemberId(int shardId, int procId) {
		this.shardId = shardId;
		this.procId = procId;
		StringBuffer buffer = new StringBuffer();
		buffer.append(shardId);
		buffer.append("_");
		buffer.append(procId);
		this.id =  buffer.toString();
	}

	public int getShardId() {
		return shardId;
	}

	public int getProcId() {
		return procId;
	}
	
}
