package cn.ict.rcc.benchmark.micro;

import java.util.ArrayList;
import java.util.Random;
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
		
		RococoTransaction t1 = fac.create();
		RococoTransaction t2 = fac.create();
		t1.begin();
		t2.begin();
		
		int num_piece1 = t1.createPiece("table1", "myKey", true);
		t1.read("myValue");
		t1.addvalue("myValue", 1);
		t1.completePiece();
		String result1 = t1.get(num_piece1, "myValue");
		LOG.debug("result1: " + result1);
		
		int num_piece2 = t2.createPiece("table1", "myKey", true);
		t2.read("myValue");
		t2.addvalue("myValue", 1);
		t2.completePiece();
		String result2 = t2.get(num_piece2, "myValue");
		LOG.debug("result1: " + result2);
		
		t2.createPiece("table2", result1, false);
		t2.write("myValue", "1");
		t2.completePiece();
		
		t1.createPiece("table2", result1, false);
		t1.write("myValue", "1");
		t1.completePiece();
		
		t2.commit();
		t1.commit();
	}
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		CoordinatorClient client = CoordinatorClient.getCoordinatorClient();
		client.callProcedure(Procedure.MICRO_BENCHMARK, new ArrayList<String>());
	}
}
