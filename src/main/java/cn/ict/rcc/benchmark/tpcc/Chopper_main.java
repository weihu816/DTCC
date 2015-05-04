package cn.ict.rcc.benchmark.tpcc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.config.AppServerConfiguration;

public class Chopper_main {

	public static AtomicInteger numTxnsAborted = new AtomicInteger(0);
	public static AtomicLong Latency = new AtomicLong(0);

	public static void main(String[] args) {
		PropertyConfigurator.configure(AppServerConfiguration.getConfiguration().getLogConfigFilePath());
		int n = 1;
		ExecutorService exec = Executors.newFixedThreadPool(n);
		Future<Integer>[] futures = new Future[n];
		int result = 0;
		for (int i = 0; i < n; i++) {
			futures[i] = exec.submit(new Chopper_task());
		}
		
		for (int i = 0; i < n; i++) {
			try {
				int x = futures[i].get();
				System.out.println("Thread" + i + ": " + x);
				result += x;
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Result: " + result);
		System.out.println("NumTxnsAborted: " + Chopper_main.numTxnsAborted);
		System.out.println("Commit Rate: " + (double) result / (result + numTxnsAborted.get()) * 100 + "%");
		System.out.println("Avg Latency: " + (double) Latency.get() / result + " ms");
		exec.shutdownNow();
	}
	
}


