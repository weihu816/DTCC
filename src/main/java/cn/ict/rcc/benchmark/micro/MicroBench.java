package cn.ict.rcc.benchmark.micro;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	private static TransactionFactory transactionFactory = new TransactionFactory();
	
//	public static void updateThreesome(int option) throws TransactionException {
//		RococoTransaction transaction = transactionFactory.create();
//		transaction.begin();
//		switch (option) {
//		case 1:
//			task1(transaction); task2(transaction); task3(transaction); break;
//		case 2:
//			task1(transaction); task3(transaction); task2(transaction); break;
//		case 3:
//			task2(transaction); task1(transaction); task3(transaction); break;
//		case 4:
//			task2(transaction); task3(transaction); task1(transaction); break;
//		case 5:
//			task3(transaction); task1(transaction); task2(transaction); break;
//		case 6:
//			task3(transaction); task2(transaction); task1(transaction); break;
//		default:
//			break;
//		}
//		transaction.commit();
//	}
	public static void updateThreesome(int option) throws TransactionException {
		RococoTransaction t1 = transactionFactory.create();
		RococoTransaction t2 = transactionFactory.create();
		RococoTransaction t3 = transactionFactory.create();

		task1(t1);
						task1(t2);
		
						task2(t2);
		task2(t1);							
		
		t1.commit();

		
//						t2.commit();
										
	}
	
	private static void task1(RococoTransaction transaction) throws TransactionException {
		int num = transaction.createPiece("table1", "myKey", true);
		transaction.addvalue("myValue", 1);
		transaction.read("myValue");
		transaction.completePiece();
		LOG.info(transaction.get(num, "myValue"));
	}

	private static void task2(RococoTransaction transaction) throws TransactionException {
		int num = transaction.createPiece("table2", "myKey", false);
		transaction.addvalue("myValue", 1);
		transaction.read("myValue");
		transaction.completePiece();
		LOG.info(transaction.get(num, "myValue"));
	}

	private static void task3(RococoTransaction transaction) throws TransactionException {
		int num = transaction.createPiece("table3", "myKey", false);
		
		transaction.addvalue("myValue", 1);
		transaction.read("myValue");
		transaction.completePiece();
		LOG.info(transaction.get(num, "myValue"));
	}
	
}
