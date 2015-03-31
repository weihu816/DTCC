package cn.ict.occ.appserver;

import java.util.List;

import org.apache.thrift.TException;

import cn.ict.occ.messaging.OCCAppServerService;
import cn.ict.occ.messaging.Option;
import cn.ict.occ.messaging.ReadValue;
import cn.ict.occ.messaging.OCCAppServerService.Iface;

public class AppServerServiceHandler implements Iface{

	private AppServer appServer;
	
	public AppServerServiceHandler(AppServer appServer) {
		this.appServer = appServer;
	}
	
	@Override
	public boolean ping() throws TException {
		return true;
	}

	@Override
	public ReadValue read(String table, String key, List<String> names)
			throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReadValue readIndexFetchTop(String table, String keyIndex,
			String orderField, boolean isAssending) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReadValue readIndexFetchMiddle(String table, String keyIndex,
			String orderField, boolean isAssending) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean commit(String transactionId, List<Option> options)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

}
