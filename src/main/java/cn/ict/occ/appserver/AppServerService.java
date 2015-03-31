package cn.ict.occ.appserver;

import java.util.Collection;
import java.util.List;

import cn.ict.occ.messaging.Result;

public interface AppServerService {
	
	public boolean ping();
	  
	Result read(String table, String key, List<String> names);
	  
	Result readIndexFetchTop(String table, String keyIndex, String orderField, boolean isAssending);
	  
	Result readIndexFetchMiddle(String table, String keyIndex, String orderField, boolean isAssending);
	
	boolean commit(String transactionId, Collection<Option> options);
	
	public void stop();
}
