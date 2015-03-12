package cn.ict.rcc.server.coordinator;

import java.util.List;

import org.apache.thrift.TException;

import cn.ict.rcc.benchmark.Procedure;
import cn.ict.rcc.benchmark.funds.FundsTransferBench;
import cn.ict.rcc.benchmark.micro.MicroBench;
import cn.ict.rcc.benchmark.tpcc.TPCC;
import cn.ict.rcc.messaging.RococoCoordinator.Iface;
import cn.ict.rcc.server.coordinator.txn.TransactionException;

public class CoordinatorServiceHandler implements Iface {

	@Override
	public void callProcedure(String procedure, List<String> paras) throws TException {
		try {
			switch (procedure) {
			case Procedure.MICRO_BENCHMARK:
				MicroBench.Micro();
				break;
			case Procedure.FUNDS_BENCHMARK:
				FundsTransferBench.FundsTransfer();
				break;
			case Procedure.TPCC_NEWORDER:
				TPCC.Neworder(Integer.valueOf(paras.get(0)), Integer.valueOf(paras.get(1)));
				break;
			case Procedure.TPCC_PAYMENT:
				TPCC.Payment(Integer.valueOf(paras.get(0)), Integer.valueOf(paras.get(1)), paras.get(2));
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
