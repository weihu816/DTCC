package cn.ict.occ.server;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;

import cn.ict.occ.messaging.Accept;
import cn.ict.occ.messaging.OCCCommunicationService.Iface;
import cn.ict.occ.messaging.ReadValue;

public class OCCCommunicationServiceHandler implements Iface{

	private StorageNode storageNode;
	
	public OCCCommunicationServiceHandler(StorageNode storageNode) {
		this.storageNode = storageNode;
	}
	
	@Override
	public boolean ping() throws TException {
		System.out.println("received ping");
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
		List<Boolean> responses = new ArrayList<Boolean>(accepts.size());
		for (Accept accept : accepts) {
			responses.add(storageNode.onAccept(toPaxosAccept(accept)));
		}
		return responses;
	}

	@Override
	public void decide(String transaction, boolean commit) throws TException {
		storageNode.onDecide(transaction, commit);
	}

	@Override
	public ReadValue read(String table, String key, List<String> names)
			throws TException {
		return storageNode.onRead(table, key, names);
	}

	@Override
	public ReadValue readIndexFetchTop(String table, String keyIndex, List<String> names,
			String orderField, boolean isAssending) throws TException {
		return storageNode.readIndexFetch(table, keyIndex, names, orderField, isAssending, "top");
	}

	@Override
	public ReadValue readIndexFetchMiddle(String table, String keyIndex, List<String> names,
			String orderField, boolean isAssending) throws TException {
		return storageNode.readIndexFetch(table, keyIndex, names, orderField, isAssending, "middle");
	}
	
	@Override
	public List<ReadValue> readIndexFetchAll(String table, String keyIndex,
			List<String> names) throws TException {
		return storageNode.readIndexFetchAll(table, keyIndex, names);
	}

	@Override
	public boolean write(String table, String key, List<String> names,
			List<String> values) throws TException {
		return storageNode.write(table, key, names, values);
	}

	@Override
	public boolean createSecondaryIndex(String table, List<String> fields)
			throws TException {
		return storageNode.createSecondaryIndex(table, fields);
	}
	
	private cn.ict.occ.appserver.Accept toPaxosAccept(Accept a) {
    	return new cn.ict.occ.appserver.Accept(a.getTransactionId(),
                a.getTable(), a.getKey(), a.getOldVersion(), a.getNames(), a.getNewValues());
    }

}
