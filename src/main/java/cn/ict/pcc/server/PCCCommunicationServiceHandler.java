package cn.ict.pcc.server;

import java.util.List;

import org.apache.thrift.TException;

import cn.ict.pcc.messaging.Accept;
import cn.ict.pcc.messaging.PCCCommunicationService.Iface;
import cn.ict.pcc.messaging.ReadValue;

public class PCCCommunicationServiceHandler implements Iface {

	private StorageNode storageNode;

	public PCCCommunicationServiceHandler(StorageNode storageNode) {
		this.storageNode = storageNode;
	}
	
	@Override
	public boolean ping() throws TException {
		return true;
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
	public ReadValue read(String txnid, String table, String key,
			List<String> names) throws TException {
		return storageNode.onRead(txnid, table, key, names);
	}

	@Override
	public boolean write(String table, String key, List<String> names,
			List<String> values) throws TException {
		return storageNode.write(table, key, names, values);
	}

	@Override
	public boolean createSecondaryIndex(String table, List<String> fields)
			throws TException {
		// TODO Auto-generated method stub
		return false;
	}

}
