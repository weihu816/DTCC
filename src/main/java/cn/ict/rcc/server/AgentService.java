package cn.ict.rcc.server;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;

public interface AgentService {
	
	public ReturnType start_req(Piece piece) throws TException;
	public ReturnType commit_req(String transactionId, Piece piece);
	public boolean write(String table, String key, List<String> names,
			List<String> values);
	public Map<String, String> execute(Piece piece) throws TException;
	
}