package cn.ict.rcc.benchmark.micro;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.PropertyConfigurator;

import cn.ict.rcc.server.coordinator.txn.CoordinatorClient;
import cn.ict.rcc.server.coordinator.txn.CoordinatorClientConfiguration;

public class MicroTest implements Runnable{

	CoordinatorClient client = CoordinatorClient.getCoordinatorClient();
	@Override
	public void run() {
		client.MicroBench();
	}
	
	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		ExecutorService exec = Executors.newFixedThreadPool(3);
		for (int i = 0; i < 3; i++) {
			exec.execute(new MicroTest());
		}
		exec.shutdownNow();
	}

}
