package cn.ict.rcc.benchmark.micro;

import org.apache.log4j.PropertyConfigurator;

import cn.ict.rcc.server.coordinator.txn.CoordinatorClient;
import cn.ict.rcc.server.coordinator.txn.CoordinatorClientConfiguration;

public class MicroTest {

	public static void main(String[] args) {

		PropertyConfigurator.configure(CoordinatorClientConfiguration
				.getConfiguration().getLogConfigFilePath());

		CoordinatorClient client = CoordinatorClient.getCoordinatorClient();
		client.MicroBench();
	}

	
}
