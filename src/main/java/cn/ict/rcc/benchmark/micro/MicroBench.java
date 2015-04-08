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

import cn.ict.dtcc.exception.TransactionException;
import cn.ict.rcc.benchmark.procedure.Procedure;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorClient;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorClientConfiguration;
import cn.ict.rcc.server.coordinator.messaging.RococoTransaction;
import cn.ict.rcc.server.coordinator.messaging.TransactionFactory;

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

	/*
	 * Micro Bench 1
	 * t1 RWi server1
	 * t2 RWi server1
	 * 
	 * t2 W Server3
	 * t1 W Server2
	 * 
	 */
	public static void Micro() throws TransactionException {
		
		final RococoTransaction t1 = fac.create();
		final RococoTransaction t2 = fac.create();

		t1.begin();
		t2.begin();
		
		int num_piece1 = t1.createPiece("table1", "myKey", true);
		t1.read("myValue");
		t1.addvalueInteger("myValue", 1);
		t1.completePiece();
		LOG.warn("result1: " + t1.get(num_piece1, "myValue"));
		String key1 = t1.get(num_piece1, "myValue");
		
		int num_piece2 = t2.createPiece("table1", "myKey", true);
		t2.read("myValue");
		t2.addvalueInteger("myValue", 1);
		t2.completePiece();
		String key2 = t2.get(num_piece2, "myValue");
		LOG.warn("result1: " + t2.get(num_piece2, "myValue"));
		
		t2.createPiece("table3", key2, false);
		t2.write("myValue", "88");
		t2.completePiece();
		
		t1.createPiece("table2", key1, false);
		t1.write("myValue", "88");
		t1.completePiece();
		
		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				try {
					t1.commit();
				} catch (TransactionException e) {
					e.printStackTrace();
				}				
			}
		});
		
		t2.commit();
		
	}
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		int n = 1;
		ExecutorService exec = Executors.newFixedThreadPool(n);
		Future<Integer>[] futures = new Future[n];
		int result = 0;
		for (int i = 0; i < n; i++) {
			futures[i] = exec.submit(new myTask());
		}
		
		for (int i = 0; i < n; i++) {
			try {
				int x = futures[i].get();
				System.out.println(x);
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

//		while (System.currentTimeMillis() - start < 10000) {
			client.callProcedure(Procedure.MICRO_BENCHMARK, new ArrayList<String>());
			count++;
//		}
		return count;

	}
}
