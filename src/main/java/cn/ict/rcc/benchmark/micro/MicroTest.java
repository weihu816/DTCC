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
//		Node n1 = new Node("x", true);
//		Node n2 = new Node("x", true);
//		Node n3 = new Node("y", true);
//		Node n4 = new Node("x", false);
//		System.out.println(n1.compareTo(n2));
//		System.out.println(n1.compareTo(n3));
//		System.out.println(n1.compareTo(n4));
//		Set<Node> set = new HashSet<Node>();
//		set.add(n1);
//		set.add(n2);
//		set.add(n3);
//		set.add(n4);
//		System.out.println(set);
	}

	
}
