package cn.ict.rcc.server.coordinator;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift.TException;

import cn.ict.dtcc.exception.TransactionException;
import cn.ict.rcc.benchmark.procedure.Procedure;
import cn.ict.rcc.benchmark.tpcc.Chopper;
import cn.ict.rcc.benchmark.tpcc.TPCC;
import cn.ict.rcc.messaging.RococoCoordinator.Iface;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorCommunicator;

public class CoordinatorServiceHandler implements Iface {

	Chopper Chopper = new Chopper() {
		
		@Override
		public Map<String, Set<String>> getServersInvolvedList() {
			// TODO Auto-generated method stub
			return null;
		}
		
		@Override
		public Map<Integer, List<String>> getReadSet() {
			// TODO Auto-generated method stub
			return null;
		}
		
		@Override
		public Map<String, String> getGraph() {
			// TODO Auto-generated method stub
			return null;
		}
		
		@Override
		public CoordinatorCommunicator getCommunicator() {
			// TODO Auto-generated method stub
			return null;
		}
	};
	@Override
	public void callProcedure(String procedure, List<String> paras) throws TException {
		try {
			switch (procedure) {
//			case Procedure.MICRO_BENCHMARK:
//				MicroBench.Micro();
//				break;
//			case Procedure.FUNDS_BENCHMARK:
//				FundsTransferBench.FundsTransfer();
//				break;
			case Procedure.TPCC_NEWORDER:
				TPCC.Neworder(Integer.valueOf(paras.get(0)), Integer.valueOf(paras.get(1)));
				break;
			case Procedure.TPCC_PAYMENT:
				TPCC.PaymentById(Integer.valueOf(paras.get(0)), Integer.valueOf(paras.get(1)), Integer.parseInt(paras.get(2)));
				break;
			case Procedure.TPCC_ORDERSTATUS:
				break;
			case Procedure.TPCC_DELIVERY:
				TPCC.Delivery(Integer.valueOf(paras.get(0)));
				break;
			case Procedure.TPCC_STOCKLEVEL:
				break;
			case Procedure.TPCC_BENCHMARK:
				break;
			default:
				break;
			}
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
