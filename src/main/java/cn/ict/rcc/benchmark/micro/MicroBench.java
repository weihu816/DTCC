package cn.ict.rcc.benchmark.micro;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
	

	private static final TransactionFactory fac = new TransactionFactory();

	public static final String ROOT = "ROOT";
	public static final String CHILD = "CHILD";

	public static void Micro(int option) throws TransactionException {
		
//		RococoTransaction t1 = transactionFactory.create();
//		RococoTransaction t2 = transactionFactory.create();
//		t1.begin();
//		t2.begin();
//		
//		int num_piece1 = t1.createPiece("table1", "myKey", true);
//		t1.read("myValue");
//		t1.addvalue("myValue", 1);
//		t1.completePiece();
//		String result1 = t1.get(num_piece1, "myValue");
//		System.err.println("result1: " + result1);
//		
//		int num_piece2 = t2.createPiece("table1", "myKey", true);
//		t2.read("myValue");
//		t2.addvalue("myValue", 1);
//		t2.completePiece();
//		String result2 = t2.get(num_piece2, "myValue");
//		System.err.println("result1: " + result2);
//		
//		t2.createPiece("table2", result1, false);
//		t2.write("myValue", "1");
//		t2.completePiece();
//		
//		t1.createPiece("table2", result1, false);
//		t1.write("myValue", "1");
//		t1.completePiece();
//		
//		t2.commit();
//		t1.commit();
				
		final int startingTotal = 100;
        final int accounts = 3;

        int num;
        
        System.err.println("Step 1: Initializing accounts");
        RococoTransaction t1 = fac.create();
        t1.begin();
        t1.createPiece("table1", "ROOT", false);
        t1.write("amount", String.valueOf(startingTotal));
        t1.completePiece();
        
        
        for (int i = 0; i < accounts; i++) {
        	t1.createPiece("table2", CHILD + i, false);
        	t1.write("amount", String.valueOf(0));
        	t1.completePiece();
        }
        t1.commit();
        System.err.println("Transaction 1 completed...");
        System.err.println("==========================================\n");

        try {
            System.err.println("Starting step 2 in 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        System.err.println("Step 2: Distributing funds");
        RococoTransaction t2 = fac.create();
        t2.begin();
        num = t2.createPiece("table1", ROOT, true);
        t2.read("amount");
        t2.completePiece();
        int root = Integer.valueOf(t2.get(num, "amount"));
        System.err.println("ROOT = " + root);
        
        int[] children = new int[accounts];
        for (int i = 0; i < accounts; i++) {
        	num = t2.createPiece("table2", CHILD + i, true);
            t2.read("amount");
            t2.completePiece();
            int child = Integer.valueOf(t2.get(num, "amount"));
            children[i] = child;
            System.err.println("CHILD" + i + " = " + child);
        }

        int fundsPerChild = root/accounts;
        for (int i = 0; i < accounts; i++) {
        	t2.createPiece("table2", CHILD + i, false);
            t2.write("amount", String.valueOf(children[i] + fundsPerChild));
            t2.completePiece();
            root -= fundsPerChild;
        }
        num = t2.createPiece("table1", ROOT, false);
        t2.write("amount", String.valueOf(root));
        t2.completePiece();
        
        t2.commit();
        System.err.println("Transaction 2 completed...");
        System.err.println("==========================================\n");
        
        
      try {
          System.out.println("Starting step 3 in 2 seconds...");
          Thread.sleep(2000);
      } catch (InterruptedException ignored) {
      }

      
      System.out.println("Step 3: Gathering funds");
      ExecutorService exec = Executors.newFixedThreadPool(accounts);
      Future[] futures = new Future[accounts];
      for (int i = 0; i < accounts; i++) {
          futures[i] = exec.submit(new FundTransferTask(fac, i));
      }

      for (int i = 0; i < accounts; i++) {
          try {
              futures[i].get();
          } catch (InterruptedException ignored) {
          } catch (ExecutionException ignored) {
          }
      }
      exec.shutdownNow();
      System.out.println("==========================================\n");

      try {
          System.out.println("Starting step 4 in 2 seconds...");
          Thread.sleep(2000);
      } catch (InterruptedException ignored) {
      }

      System.out.println("Step 4: Validating funds");
      RococoTransaction t4 = fac.create();
      t4.begin();
      int total = 0;
      num = t4.createPiece("table1", ROOT, true);
      t4.read("amount");
      t4.completePiece();
      root = Integer.valueOf(t4.get(num, "amount"));
      System.out.println("ROOT = " + root);
      children = new int[accounts];
      total += root;
      for (int i = 0; i < accounts; i++) {
    	  num = t4.createPiece("table2", CHILD + i, true);
          t4.read("amount");
          t4.completePiece();
          int child = Integer.valueOf(t4.get(num, "amount"));
          children[i] = child;
          total += child;
          System.out.println(CHILD + i + " = " + child);
      }

      t4.commit();
      System.out.println("Transaction 4 completed...");
      System.out.println("==========================================\n");
      System.out.println("FINAL TOTAL = " + total);
        
	}
	
	private static class FundTransferTask implements Runnable {

		private TransactionFactory fac;
		private int index;

		private FundTransferTask(TransactionFactory fac, int index) {
			this.fac = fac;
			this.index = index;
		}

		public void run() {
			try {
				try {
					Thread.sleep(new Random().nextInt(10) * 500);
				} catch (InterruptedException ignored) {
				}
				RococoTransaction t3 = fac.create();
				t3.begin();
				int num = t3.createPiece("table1", ROOT, true);
				t3.read("amount");
				t3.completePiece();
				int root = Integer.valueOf(t3.get(num, "amount"));
				System.out.println("ROOT = " + root);
				
				num = t3.createPiece("table2", CHILD + index, true);
				t3.read("amount");
				t3.completePiece();
				int child = Integer.valueOf(t3.get(num, "amount"));
				System.out.println(CHILD + index + " = " + child);

				num = t3.createPiece("table1", ROOT, false);
				t3.write("amount", String.valueOf(root + child));
				t3.completePiece();
				
				num = t3.createPiece("table2", CHILD + index, false);
				t3.write("amount", String.valueOf(0));
				t3.completePiece();
				t3.commit();
				System.out.println("Transaction 3 completed by Thread-" + index);
			} catch (TransactionException e) {
				System.out.println("Transaction 3 failed by Thread-" + index);
			}
		}
	}
}
