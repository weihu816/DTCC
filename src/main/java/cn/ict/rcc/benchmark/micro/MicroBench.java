package cn.ict.rcc.benchmark.micro;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import cn.ict.rcc.benchmark.Procedure;
import cn.ict.rcc.server.coordinator.txn.CoordinatorClient;
import cn.ict.rcc.server.coordinator.txn.CoordinatorClientConfiguration;
import cn.ict.rcc.server.coordinator.txn.RococoTransaction;
import cn.ict.rcc.server.coordinator.txn.TransactionException;
import cn.ict.rcc.server.coordinator.txn.TransactionFactory;

/**
 * MicroBenchmark for rcc
 * 
 * @author Wei
 *
 */
public class MicroBench {
	
	private static final Log LOG = LogFactory.getLog(MicroBench.class);
	
	private static final TransactionFactory fac = new TransactionFactory();

	public static final String ROOT = "ROOT";
	public static final String CHILD = "CHILD";

	public static void Micro() throws TransactionException {
		
		final RococoTransaction t1 = fac.create();
		
		t1.begin();
		
		int num_piece1 = t1.createPiece("table1", "myKey", true);
		t1.addvalueInteger("myValue", 1);
		t1.read("myValue");
		t1.completePiece();
		LOG.debug("result1: " + t1.get(num_piece1, "myValue"));
	
		t1.createPiece("table2", "myKey", false);
		t1.write("myValue", "1");
		t1.read("myValue");
		t1.completePiece();
		
		t1.commit();

	}
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		int n = 4;
		ExecutorService exec = Executors.newFixedThreadPool(n);
		Future<Integer>[] futures = new Future[n];
		int result = 0;
		for (int i = 0; i < n; i++) {
			futures[i] = exec.submit(new myTask());
		}
		
		for (int i = 0; i < n; i++) {
			try {
				int x = futures[i].get();
				System.err.println(x);
				result += x;
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.println(result);
		exec.shutdownNow();
	}
	
}

class myTask implements Callable<Integer> {

	@Override
	public Integer call() throws Exception {
		
		int count = 0;
		long start = System.currentTimeMillis();
		CoordinatorClient client = CoordinatorClient.getCoordinatorClient();

		while (System.currentTimeMillis() - start < 10000) {
			client.callProcedure(Procedure.MICRO_BENCHMARK, new ArrayList<String>());
			count++;
		}
		return count;

	}
}
