package cn.ict.occ.server;

import java.util.List;

import org.apache.thrift.TException;

import cn.ict.occ.messaging.Accept;
import cn.ict.occ.messaging.OCCCommunicationService;
import cn.ict.occ.messaging.ReadValue;
import cn.ict.occ.messaging.OCCCommunicationService.Iface;

public class OCCCommunicationServiceHandler implements Iface{

	private StorageNode storageNode;
	
	public OCCCommunicationServiceHandler(StorageNode storageNode) {
		this.storageNode = storageNode;
	}
	
	@Override
	public boolean ping() throws TException {
		return true;
	}

	@Override
	public boolean prepare(String table, String key) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean accept(Accept accept) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<Boolean> bulkAccept(List<Accept> accepts) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void decide(String transaction, boolean commit) throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ReadValue read(String table, String key, List<String> names)
			throws TException {
		return storageNode.onRead(table, key, names);
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

}
