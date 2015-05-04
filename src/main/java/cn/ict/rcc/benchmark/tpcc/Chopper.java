package cn.ict.rcc.benchmark.tpcc;

import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.ict.rcc.server.coordinator.messaging.CoordinatorCommunicator;

public interface Chopper {

	public CoordinatorCommunicator getCommunicator();
	
	public Map<Integer, List<String>> getReadSet();
	
	public Map<String, String> getGraph();
	
	public Map<String, Set<String>> getServersInvolvedList();
}
