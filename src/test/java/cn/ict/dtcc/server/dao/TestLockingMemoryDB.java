package cn.ict.dtcc.server.dao;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

public class TestLockingMemoryDB extends LockingMemoryDB {

	private static final Log log = LogFactory.getLog(TestLockingMemoryDB.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		PropertyConfigurator.configure("conf/log4j-server.properties");

		TestLockingMemoryDB db = new TestLockingMemoryDB();
		int threadNum = 4;
		ExecutorService exec = Executors.newFixedThreadPool(threadNum);
		Future<Integer>[] futures = new Future[threadNum];
		for (int i = 0; i < threadNum; i++) {
			futures[i] = exec.submit(new TPCCTerminal(db, i));
		}

		for (int i = 0; i < threadNum; i++) {
			try {
				futures[i].get(120, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
		}
		exec.shutdownNow();
	}

	private static class TPCCTerminal implements Callable<Integer> {

		TestLockingMemoryDB db;
		static Random rand = new Random();
		static String table = "warehouse";
		int id;

		private TPCCTerminal(TestLockingMemoryDB db, int procId) {
			this.db = db;
			this.id = procId;
		}

		@Override
		public Integer call() throws Exception {
			String tid = String.valueOf(rand.nextLong());
			this.db.locksAppend(tid, table, new String[] { "w_tax" });
			while (true) {
				if (this.db.isNonConflictingHead(table, tid)) {
					log.info("Lock Success! ====" + id);
					Thread.sleep(rand.nextInt(1000));
					this.db.locksRemove(table, tid);
				} else {
					long sleepingTime = (long) (rand.nextInt(100) * 100);
					log.info("Sleeping for " + sleepingTime + "====" + id);
					Thread.sleep(sleepingTime);
				}
			}
		}

	}

}
