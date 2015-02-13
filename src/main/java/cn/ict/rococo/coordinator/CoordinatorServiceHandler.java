package cn.ict.rococo.coordinator;

import java.util.List;

import org.apache.thrift.TException;

import cn.ict.rococo.messaging.Piece;
import cn.ict.rococo.messaging.RococoCoordinator.Iface;
import cn.ict.rococo.procedure.TransactionException;

public class CoordinatorServiceHandler implements Iface {

	private Coordinator coordinator;
	
	public CoordinatorServiceHandler(Coordinator coordinator) {
		this.coordinator = coordinator;
	}

	@Override
	public void NewOrder(int w_id, int d_id) throws TException {
		try {
			coordinator.TPCC_NewOrder(w_id, d_id);
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
		// TODO Auto-generated method stub
		return false;
	}


	

}
