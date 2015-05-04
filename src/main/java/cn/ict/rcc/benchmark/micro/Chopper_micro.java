package cn.ict.rcc.benchmark.micro;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.log4j.PropertyConfigurator;

import cn.ict.dtcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.config.Member;
import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.StartResponseBulk;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.coordinator.messaging.CommitListener;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorCommunicator;

public class Chopper_micro {

	public static final int MICRO_0 = 0;
	public static final int MICRO_1 = 1;
	public static final String TABLE1 = "table1";
	public static final String TABLE2 = "table2";
	public static final String TABLE3 = "table3";
	
	public Chopper_micro(CoordinatorCommunicator communicator) {
		
	}

	public static Piece MICRO_0(String transactionId) {
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId,
				TABLE1, "myKey", true, MICRO_0);
		Vertex v;
		// read
		v = new Vertex(Action.READSELECT);
		v.setName(TPCCGenerator.buildColumns("myValue"));
		piece.getVertexs().add(v);
		// update
		v = new Vertex(Action.ADDI);
		v.setName(TPCCGenerator.buildColumns("myValue"));
		v.setValue(TPCCGenerator.buildColumns("1"));
		piece.getVertexs().add(v);

		return piece;
	}

	public static Piece MICRO_1(String transactionId, String table, String myKey) {
		Piece piece = new Piece(new ArrayList<Vertex>(), transactionId,
				table, myKey, false, MICRO_1);
		Vertex v = new Vertex(Action.WRITE);
		v.setName(TPCCGenerator.buildColumns("myValue"));
		v.setValue(TPCCGenerator.buildColumns("88"));
		piece.getVertexs().add(v);
		v = new Vertex(Action.READSELECT);
		v.setName(TPCCGenerator.buildColumns("myValue"));
		piece.getVertexs().add(v);
		return piece;
	}

	public static void main(String[] args) {
		
		AppServerConfiguration config = AppServerConfiguration.getConfiguration();
		PropertyConfigurator.configure(AppServerConfiguration.getConfiguration().getLogConfigFilePath());

		Map<String, String> vertexes;
		Member member;

		final Map<String, String> dep0 = new ConcurrentHashMap<String, String>();
		final Map<String, Set<String>> serversInvolvedList0 = new ConcurrentHashMap<String, Set<String>>();
		final Map<Integer, List<String>> readSet0 = new ConcurrentHashMap<Integer, List<String>>();
		final Set<Member> members0 = new HashSet<Member>();
		final CoordinatorCommunicator communicator0 = new CoordinatorCommunicator();
		
		Map<String, String> dep1 = new ConcurrentHashMap<String, String>();
		Map<String, Set<String>> serversInvolvedList1 = new ConcurrentHashMap<String, Set<String>>();
		Map<Integer, List<String>> readSet1 = new ConcurrentHashMap<Integer, List<String>>();
		Set<Member> members1 = new HashSet<Member>();
		CoordinatorCommunicator communicator1 = new CoordinatorCommunicator();

		
		Piece pieces0_0 = Chopper_micro.MICRO_0("0");
		Piece pieces1_0 = MICRO_0("1");

		member = config.getShardMember(pieces0_0.getTable(), pieces0_0.getKey());
		members0.add(member);
		StartResponseBulk result0_0 = communicator0.firstRound(member, pieces0_0);
		readSet0.put(MICRO_0, result0_0.getOutput().get(0));
		dep0.putAll(result0_0.getDep().getVertexes());
		vertexes = result0_0.getDep().getVertexes();
		for (Entry<String, String> entry : vertexes.entrySet()) {
			String key = entry.getValue();
			Set<String> set = serversInvolvedList0.get(key);
			if (set == null) {
				set = new HashSet<String>();
				serversInvolvedList0.put(key, set);
			}
			set.add(member.getId());
		}
		
		member = config.getShardMember(pieces1_0.getTable(), pieces1_0.getKey());
		members1.add(member);
		StartResponseBulk result1_0 = communicator1.firstRound(member, pieces1_0);
		readSet1.put(MICRO_0, result1_0.getOutput().get(0));
		dep1.putAll(result1_0.getDep().getVertexes());
		vertexes = result1_0.getDep().getVertexes();
		for (Entry<String, String> entry : vertexes.entrySet()) {
			String key = entry.getValue();
			Set<String> set = serversInvolvedList1.get(key);
			if (set == null) {
				set = new HashSet<String>();
				serversInvolvedList1.put(key, set);
			}
			set.add(member.getId());
		}

		System.out.println("*** txn 0: " + readSet0.get(MICRO_0).get(0));
		System.out.println("*** txn 1: " + readSet1.get(MICRO_0).get(0));

		//=====================================================================
		
		Piece pieces0_1 = MICRO_1("0", TABLE2, readSet0.get(MICRO_0).get(0));
		Piece pieces1_1 = MICRO_1("1", TABLE3, readSet1.get(MICRO_0).get(0));
		
		member = config.getShardMember(pieces1_1.getTable(), pieces1_1.getKey());
		members1.add(member);
		StartResponseBulk result1_1 = communicator1.firstRound(member, pieces1_1);
		readSet1.put(MICRO_0, result1_1.getOutput().get(0));
		dep1.putAll(result1_1.getDep().getVertexes());
		vertexes = result1_1.getDep().getVertexes();
		for (Entry<String, String> entry : vertexes.entrySet()) {
			String key = entry.getValue();
			Set<String> set = serversInvolvedList1.get(key);
			if (set == null) {
				set = new HashSet<String>();
				serversInvolvedList1.put(key, set);
			}
			set.add(member.getId());
		}

		member = config.getShardMember(pieces0_1.getTable(), pieces0_1.getKey());
		members0.add(member);
		StartResponseBulk result0_1 = communicator0.firstRound(member, pieces0_1);
		readSet1.put(MICRO_0, result0_1.getOutput().get(0));
		dep1.putAll(result0_1.getDep().getVertexes());
		vertexes = result0_1.getDep().getVertexes();
		for (Entry<String, String> entry : vertexes.entrySet()) {
			String key = entry.getValue();
			Set<String> set = serversInvolvedList1.get(key);
			if (set == null) {
				set = new HashSet<String>();
				serversInvolvedList1.put(key, set);
			}
			set.add(member.getId());
		}
		
		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				// tx0 commit
				String transactionId = "0";
				Graph graph0 = new Graph();
				graph0.setVertexes(dep0);
				graph0.setServersInvolved(serversInvolvedList0);
				CommitListener commitListener = new CommitListener(members0, communicator0, transactionId, graph0);
				commitListener.start();
				synchronized (commitListener) {
					long start = System.currentTimeMillis();
					while (commitListener.getCount() < members0.size()) {
						try {
							System.out.println("Transaction " + transactionId + " waiting for commit");
							commitListener.wait(2000);
						} catch (InterruptedException ignored) {
						}

						if (System.currentTimeMillis() - start > 10000) {
							System.out.println("Transaction " + transactionId + " timed out");
							break;
						}
					}
				}				
			}
		});

		// tx1 commit
		String transactionId = "1";
		Graph graph1 = new Graph();
		graph1.setVertexes(dep1);
		graph1.setServersInvolved(serversInvolvedList1);
		CommitListener commitListener = new CommitListener(members1, communicator1, transactionId, graph1);
		commitListener.start();
		synchronized (commitListener) {
			long start = System.currentTimeMillis();
			while (commitListener.getCount() < members1.size()) {
				try {
					System.out.println("Transaction " + transactionId + " waiting for commit");
					commitListener.wait(2000);
				} catch (InterruptedException ignored) {
				}

				if (System.currentTimeMillis() - start > 10000) {
					System.out.println("Transaction " + transactionId + " timed out");
					break;
				}
			}
		}
	}
}
