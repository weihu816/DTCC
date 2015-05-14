package cn.ict.rcc.benchmark.tpcc;

import java.util.concurrent.Callable;

import org.apache.thrift.TException;

import cn.ict.dtcc.benchmark.tpcc.TPCCConstants;
import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorCommunicator;

public class Chopper_task implements Callable<Integer> {

	CoordinatorCommunicator communicator = new CoordinatorCommunicator();
	@Override
	public Integer call() throws Exception {
		long start = System.currentTimeMillis();
		int count_neworder = 0;

		while (System.currentTimeMillis() - start < 30000) {

			long xxx = System.currentTimeMillis();
			int x = TPCCGenerator.randomInt(0, 99);
			x = 1;
			if (x <= 47) {
				int w_id = TPCCGenerator.randomInt(1,TPCCConstants.NUM_WAREHOUSE);
				int d_id = TPCCGenerator.randomInt(1,TPCCConstants.DISTRICTS_PER_WAREHOUSE);
				Chopper_neworder chopper = new Chopper_neworder(w_id, d_id, communicator);
				try {
					chopper.run();
				} catch (TException e) {
					e.printStackTrace();
				}
			} else if (x <= 93) {
				if (TPCCGenerator.randomInt(0, 1) == 0) {

				} else {

				}
			} else {

			}
			Chopper_main.Latency.addAndGet(System.currentTimeMillis() - xxx);
			count_neworder++;

//			try {
//				System.out.println("Starting another txn in 1 seconds...");
//				Thread.sleep(1000);
//			} catch (InterruptedException ignored) {
//			}
		}
		return count_neworder;
	}
}
