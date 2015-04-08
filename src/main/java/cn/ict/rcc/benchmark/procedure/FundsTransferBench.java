package cn.ict.rcc.benchmark.procedure;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.exception.TransactionException;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorClient;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorClientConfiguration;
import cn.ict.rcc.server.coordinator.messaging.RococoTransaction;
import cn.ict.rcc.server.coordinator.messaging.TransactionFactory;

public class FundsTransferBench {
	
	private static final Log LOG = LogFactory.getLog(FundsTransferBench.class);

	private static final TransactionFactory fac = new TransactionFactory();
	
    public static final String ROOT = "ROOT";
    public static final String CHILD = "CHILD";

    public static void FundsTransfer() throws TransactionException  {

    	final int startingTotal = 100;
        final int accounts = 50;

        int num;
        
        LOG.debug("Step 1: Initializing accounts");
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
        LOG.debug("Transaction 1 completed...");
        LOG.debug("==========================================\n");

        try {
            LOG.debug("Starting step 2 in 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        LOG.debug("Step 2: Distributing funds");
        RococoTransaction t2 = fac.create();
        t2.begin();
        num = t2.createPiece("table1", ROOT, true);
        t2.read("amount");
        t2.completePiece();
        int root = Integer.valueOf(t2.get(num, "amount"));
        LOG.debug("ROOT = " + root);
        
        int[] children = new int[accounts];
        for (int i = 0; i < accounts; i++) {
        	num = t2.createPiece("table2", CHILD + i, true);
            t2.read("amount");
            t2.completePiece();
            int child = Integer.valueOf(t2.get(num, "amount"));
            children[i] = child;
            LOG.debug("CHILD" + i + " = " + child);
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
        LOG.debug("Transaction 2 completed...");
        LOG.debug("==========================================\n");
        
        
      try {
          LOG.debug("Starting step 3 in 2 seconds...");
          Thread.sleep(2000);
      } catch (InterruptedException ignored) {
      }

      
      LOG.debug("Step 3: Gathering funds");
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
      LOG.debug("==========================================\n");

      try {
          LOG.debug("Starting step 4 in 2 seconds...");
          Thread.sleep(2000);
      } catch (InterruptedException ignored) {
      }

      LOG.debug("Step 4: Validating funds");
      RococoTransaction t4 = fac.create();
      t4.begin();
      int total = 0;
      num = t4.createPiece("table1", ROOT, true);
      t4.read("amount");
      t4.completePiece();
      root = Integer.valueOf(t4.get(num, "amount"));
      LOG.debug("ROOT = " + root);
      children = new int[accounts];
      total += root;
      for (int i = 0; i < accounts; i++) {
    	  num = t4.createPiece("table2", CHILD + i, true);
          t4.read("amount");
          t4.completePiece();
          int child = Integer.valueOf(t4.get(num, "amount"));
          children[i] = child;
          total += child;
          LOG.debug(CHILD + i + " = " + child);
      }

      t4.commit();
      LOG.debug("Transaction 4 completed...");
      LOG.debug("==========================================\n");
      LOG.debug("FINAL TOTAL = " + total);
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
				
				int num;
//				num = t3.createPiece("table1", ROOT, true);
//				t3.read("amount");
//				t3.completePiece();
//				int root = Integer.valueOf(t3.get(num, "amount"));
//				LOG.debug("ROOT = " + root);
				
				num = t3.createPiece("table2", CHILD + index, true);
				t3.read("amount");
				t3.completePiece();
				int child = Integer.valueOf(t3.get(num, "amount"));
				LOG.debug(CHILD + index + " = " + child);

				num = t3.createPiece("table1", ROOT, false);
//				t3.write("amount", String.valueOf(root + child));
				t3.addvalueInteger("amount", child);
				t3.completePiece();
				
				num = t3.createPiece("table2", CHILD + index, false);
				t3.write("amount", String.valueOf(0));
				t3.completePiece();
				t3.commit();
				LOG.debug("Transaction 3 completed by Thread-" + index);
			} catch (TransactionException e) {
				LOG.debug("Transaction 3 failed by Thread-" + index);
			}
		}
	}
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		CoordinatorClient client = CoordinatorClient.getCoordinatorClient();
		client.callProcedure(Procedure.FUNDS_BENCHMARK, new ArrayList<String>());
	}
}
