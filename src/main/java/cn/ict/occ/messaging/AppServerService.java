package cn.ict.occ.messaging;

import java.util.List;

import cn.ict.occ.appserver.Result;

public interface AppServerService {
	
	public boolean ping();
	  
	Result read(String table, String key, List<String> names);
	  
	Result readIndexFetchTop(String table, String keyIndex, String orderField, boolean isAssending);
	  
	Result readIndexFetchMiddle(String table, String keyIndex, String orderField, boolean isAssending);
	
	public void stop();
}
