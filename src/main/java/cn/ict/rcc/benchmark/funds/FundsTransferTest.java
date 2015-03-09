package cn.ict.rcc.benchmark.funds;

import cn.ict.rcc.server.coordinator.txn.RococoTransaction;
import cn.ict.rcc.server.coordinator.txn.TransactionException;
import cn.ict.rcc.server.coordinator.txn.TransactionFactory;

public class FundsTransferTest {

	private static final TransactionFactory fac = new TransactionFactory();
	
    public static final String ROOT = "ROOT";
    public static final String CHILD = "CHILD";

    public static void main(String[] args) throws TransactionException  {

        final int startingTotal = 100;
        final int accounts = 4;

        int num;
        
        System.out.println("Step 1: Initializing accounts");
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
        System.out.println("Transaction 1 completed...");
        System.out.println("==========================================\n");

        try {
            System.out.println("Starting step 2 in 2 seconds...");
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        System.out.println("Step 2: Distributing funds");
        RococoTransaction t2 = fac.create();
        t2.begin();
        num = t2.createPiece("table1", ROOT, true);
        t2.read("amount");
        t2.completePiece();
        int root = Integer.valueOf(t2.get(num, "amount"));
        System.out.println("ROOT = " + root);
        
        int[] children = new int[accounts];
        for (int i = 0; i < accounts; i++) {
        	num = t2.createPiece("table2", CHILD + i, true);
            t2.read("amount");
            t2.completePiece();
            int child = Integer.valueOf(t2.get(num, "amount"));
            children[i] = child;
            System.out.println("CHILD" + i + " = " + child);
        }

        int fundsPerChild = root/accounts;
        for (int i = 0; i < accounts; i++) {
        	t2.createPiece("table2", CHILD + i, true);
            t2.write("amount", String.valueOf(children[i] + fundsPerChild));
            t2.completePiece();
            root -= fundsPerChild;
        }
        num = t2.createPiece("table1", ROOT, true);
        t2.write("amount", String.valueOf(root));
        t2.completePiece();
        
        t2.commit();
        System.out.println("Transaction 2 completed...");
        System.out.println("==========================================\n");
//
//        try {
//            System.out.println("Starting step 3 in 2 seconds...");
//            Thread.sleep(2000);
//        } catch (InterruptedException ignored) {
//        }
//
//        System.out.println("Step 3: Gathering funds");
//        ExecutorService exec = Executors.newFixedThreadPool(accounts);
//        Future[] futures = new Future[accounts];
//        for (int i = 0; i < accounts; i++) {
//            futures[i] = exec.submit(new FundTransferTask(fac, i));
//        }
//
//        for (int i = 0; i < accounts; i++) {
//            try {
//                futures[i].get();
//            } catch (InterruptedException ignored) {
//            } catch (ExecutionException ignored) {
//            }
//        }
//        exec.shutdownNow();
//        System.out.println("==========================================\n");
//
//        try {
//            System.out.println("Starting step 4 in 2 seconds...");
//            Thread.sleep(2000);
//        } catch (InterruptedException ignored) {
//        }
//
//        System.out.println("Step 4: Validating funds");
//        Transaction t4 = fac.create();
//        t4.begin();
//        int total = 0;
//        root = toInt(t4.read(ROOT));
//        System.out.println("ROOT = " + root);
//        children = new int[accounts];
//        total += root;
//        for (int i = 0; i < accounts; i++) {
//            int child = toInt(t4.read(CHILD + i));
//            children[i] = child;
//            total += child;
//            System.out.println(CHILD + i + " = " + child);
//        }
//
//        t4.commit();
//        System.out.println("Transaction 4 completed...");
//        System.out.println("==========================================\n");
//        System.out.println("FINAL TOTAL = " + total);
    }

//    private static class FundTransferTask implements Runnable {
//
//        private TransactionFactory fac;
//        private int index;
//
//        private FundTransferTask(TransactionFactory fac, int index) {
//            this.fac = fac;
//            this.index = index;
//        }
//
//        public void run() {
//            try {
//                try {
//                    Thread.sleep(new Random().nextInt(10) * 500);
//                } catch (InterruptedException ignored) {
//                }
//                Transaction t3 = fac.create();
//                t3.begin();
//                int root = toInt(t3.read(ROOT));
//                System.out.println("ROOT = " + root);
//                int child = toInt(t3.read(CHILD + index));
//                System.out.println(CHILD + index + " = " + child);
//
//                t3.write(ROOT, toBytes(root + child));
//                t3.write(CHILD + index, toBytes(0));
//                t3.commit();
//                System.out.println("Transaction 3 completed by Thread-" + index);
//            } catch (TransactionException e) {
//                System.out.println("Transaction 3 failed by Thread-" + index);
//            }
//        }
//    }

}
