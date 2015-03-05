package cn.ict.rcc.benchmark.micro;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.PropertyConfigurator;

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

	static TransactionFactory transactionFactory = new TransactionFactory();

	public static void updateThreesome() throws TransactionException {
		RococoTransaction transaction = transactionFactory.create();
		transaction.begin();

		int num;

		num = transaction.createPiece("table1", "myKey", true);
		transaction.read("myValue");
		transaction.addvalue("myValue", 1);
		transaction.completePiece();
		System.out.println(transaction.get(num, "myValue"));

		num = transaction.createPiece("table2", "myKey", true);
		transaction.read("myValue");
		transaction.addvalue("myValue", 1);
		transaction.completePiece();
		System.out.println(transaction.get(num, "myValue"));

		num = transaction.createPiece("table3", "myKey", true);
		transaction.read("myValue");
		transaction.addvalue("myValue", 1);
		transaction.completePiece();
		System.out.println(transaction.get(num, "myValue"));

		transaction.commit();

	}
	
}
