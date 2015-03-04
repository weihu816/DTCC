package cn.ict.rcc.server.coordinator;

import java.util.List;

import org.apache.thrift.TException;

import cn.ict.rcc.benchmark.micro.MicroBench;
import cn.ict.rcc.benchmark.tpcc.TPCC;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.RococoCoordinator.Iface;
import cn.ict.rcc.procedure.TransactionException;

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
			MicroBench.updataThreesome();
		} catch (TransactionException e) {
			e.printStackTrace();
		}
	}	
	
	@Override
	public boolean ping() throws TException {
		System.out.println("received ping");
		return true;
	}

	@Override
	public boolean commit(String transactionId, List<Piece> pieces)
			throws TException {
		return false;
	}

}
