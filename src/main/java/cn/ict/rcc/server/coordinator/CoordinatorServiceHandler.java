package cn.ict.rcc.server.coordinator;

import org.apache.thrift.TException;

import cn.ict.rcc.benchmark.micro.MicroBench;
import cn.ict.rcc.benchmark.tpcc.TPCC;
import cn.ict.rcc.messaging.RococoCoordinator.Iface;
import cn.ict.rcc.server.coordinator.txn.TransactionException;

public class CoordinatorServiceHandler implements Iface {

	@Override
	public void procedure_newOrder(int w_id, int d_id) throws TException {
		try {
			TPCC.Neworder(w_id, d_id);
		} catch (TransactionException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void procedure_micro() throws TException {
		try {
			MicroBench.Micro(1);
		} catch (TransactionException e) {
			e.printStackTrace();
		}
	}	
	
	@Override
	public boolean ping() throws TException {
		System.out.println("received ping");
		return true;
	}

}
