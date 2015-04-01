package cn.ict.occ.txn;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.dtcc.exception.TransactionException;
import cn.ict.dtcc.util.DTCCUtil;
import cn.ict.occ.appserver.Transaction;

public class FundsTransferTest {

    public static final String ROOT = "ROOT";
    public static final String CHILD = "CHILD";
    public static final String TABLE = "TABLE";
    public static final List<String> NAMES = DTCCUtil.buildColumns("VALUE");

    public static void main(String[] args) throws TransactionException {
    	
    	PropertyConfigurator.configure(ServerConfiguration.getConfiguration().getLogConfigFilePath());

    	
        final TransactionFactory fac = new TransactionFactory();

//        final int startingTotal = Integer.parseInt(args[0]);
//        final int accounts = Integer.parseInt(args[1]);
      final int startingTotal = 100;
      final int accounts = 10;

        List<String> values;

        System.out.println("Step 1: Initializing accounts");
        Transaction t1 = fac.create();
        t1.begin();
    	values = DTCCUtil.buildColumns(startingTotal);
        t1.write(TABLE, ROOT, NAMES, values);
        for (int i = 0; i < accounts; i++) {
        	values = DTCCUtil.buildColumns(0);
            t1.write("TABLE", CHILD + i, NAMES, values);
        }
        t1.commit();
        System.out.println("Transaction 1 completed...");
        System.out.println("==========================================\n");

        try {
            System.out.println("Starting step 2 in 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        System.out.println("Step 2: Distributing funds");
        Transaction t2 = fac.create();
        t2.begin();
        int root = Integer.parseInt(t2.read(TABLE, ROOT, NAMES).get(0));
        System.out.println("ROOT = " + root);
        int[] children = new int[accounts];
        for (int i = 0; i < accounts; i++) {
            int child = Integer.parseInt(t2.read(TABLE, CHILD + i, NAMES).get(0));
            children[i] = child;
            System.out.println("CHILD" + i + " = " + child);
        }

        int fundsPerChild = root/accounts;
        for (int i = 0; i < accounts; i++) {
        	values = DTCCUtil.buildColumns(children[i] + fundsPerChild);
            t2.write(TABLE, CHILD + i, NAMES, values);
            root -= fundsPerChild;
        }
    	values = DTCCUtil.buildColumns(root);
        t2.write(TABLE, ROOT, NAMES, values);
        t2.commit();
        System.out.println("Transaction 2 completed...");
        System.out.println("==========================================\n");

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
        Transaction t4 = fac.create();
        t4.begin();
        int total = 0;
        root = Integer.parseInt(t4.read(TABLE, ROOT, NAMES).get(0));
        System.out.println("ROOT = " + root);
        children = new int[accounts];
        total += root;
        for (int i = 0; i < accounts; i++) {
            int child = Integer.parseInt(t4.read(TABLE, CHILD + i, NAMES).get(0));
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
                Transaction t3 = fac.create();
                t3.begin();
                int root = Integer.parseInt(t3.read(TABLE, ROOT, NAMES).get(0));
                System.out.println("ROOT = " + root);
                int child = Integer.parseInt(t3.read(TABLE, CHILD + index, NAMES).get(0));
                System.out.println(CHILD + index + " = " + child);
                
            	List<String> values = DTCCUtil.buildColumns(root + child);
                t3.write(TABLE, ROOT, NAMES, values);
                values = DTCCUtil.buildColumns(0);
                t3.write(TABLE, CHILD + index, NAMES, values);
                t3.commit();
                System.out.println("Transaction 3 completed by Thread-" + index);
            } catch (TransactionException e) {
                System.out.println("Transaction 3 failed by Thread-" + index);
            }
        }
    }

}
