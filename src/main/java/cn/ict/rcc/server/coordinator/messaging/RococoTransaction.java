package cn.ict.rcc.server.coordinator.messaging;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.dtcc.config.AppServerConfiguration;
import cn.ict.dtcc.config.Member;
import cn.ict.dtcc.exception.TransactionException;
import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.StartResponse;
import cn.ict.rcc.messaging.Vertex;

public class RococoTransaction {
	
	private static final Log LOG = LogFactory.getLog(RococoTransaction.class);

	private AppServerConfiguration config = AppServerConfiguration.getConfiguration();

	protected String transactionId;
	protected boolean finished = false;
	private Piece piece = null;
	private List<Piece> pieces = new ArrayList<Piece>();
	CoordinatorCommunicator communicator;
	int piece_number;
	private Map<Integer, List<Map<String, String>>> readSet = new ConcurrentHashMap<Integer, List<Map<String, String>>>();
	private Map<Integer, Boolean> readFlag= new ConcurrentHashMap<Integer, Boolean>();
	Map<String, String> dep = new HashMap<String, String>();
	Map<String, Set<String>> serversInvolvedList = new HashMap<String, Set<String>>();

	public RococoTransaction() {
//		communicator = CoordinatorCommunicator.getCoordinatorCommunicator();
		communicator = new CoordinatorCommunicator();
	}
	
	public void begin() {
//		transactionId = UUID.randomUUID().toString();
		transactionId = String.valueOf(TransactionFactory.transactionIdGen.addAndGet(1));
		piece_number = 0;
	}
	
	public int createPiece(String table, String key, boolean immediate) throws TransactionException {
		assertState();
		if (piece != null) {
			throw new TransactionException("Create a new piece without complete the last one");
		}
		piece = new Piece(new ArrayList<Vertex>(), transactionId, table, key, immediate);
		piece_number++;
		return piece_number;
	}
	
	public void completePiece() {
		pieces.add(piece);
		Piece tempPiece = piece;
		Member member = config.getShardMember(piece.getTable(), piece.getKey());
		StartResponse startResponse = communicator.fistRound(member, tempPiece);
		
		List<Map<String, String>> map = readSet.get(piece_number);
		if (map == null) {
			map = new ArrayList<Map<String, String>>();
			readSet.put(piece_number, map);
		}
		map.addAll(startResponse.getOutput());
		dep.putAll(startResponse.getDep().getVertexes());
		
		// ServersInvolved
		Map<String, String> vertexes = startResponse.getDep().getVertexes();
		for (Entry<String, String> entry : vertexes.entrySet()) {
			String key = entry.getValue();
			Set<String> set = serversInvolvedList.get(key);
			if (set == null) {
				set = new HashSet<String>();
				serversInvolvedList.put(key, set);
			}
			set.add(member.getId());
		}
		
		readFlag.put(piece_number, true);
		LOG.debug(startResponse);
		LOG.debug("dep after: " + dep);
		piece = null;
	}
	
	public boolean commit() throws TransactionException {
		LOG.info("Commit: " + transactionId);
		if (piece != null) {
			throw new TransactionException("Commit a txn without complete the pieces");
		}
		Graph graph = new Graph();
		graph.setVertexes(dep);
		graph.setServersInvolved(serversInvolvedList);
		// Get all members involved in this transaction
		Set<Member> members = new HashSet<Member>();
		for (Piece p : pieces) {
			members.add(config.getShardMember(p.getTable(), p.getKey()));
		}
		
		// send out asynchronized to all nodes to commit transaction
		CommitListener commitListener = new CommitListener(members, communicator, transactionId, graph);
		commitListener.start();
		
		// wait until all responses received and transaction done
		synchronized (commitListener) {
			long start = System.currentTimeMillis();
			while (commitListener.getCount() < members.size()) {
			    try {
			        LOG.warn("Transaction " + transactionId + " waiting for commit");
			        commitListener.wait(5000);
				} catch (InterruptedException ignored) { }

			    if (System.currentTimeMillis() - start > 15000) {
			        LOG.warn("Transaction " + transactionId + " timed out");
			        break;
			    }
			}
		}
		// now simply remove the transaction from the dep graph
		dep.remove(transactionId);
		LOG.debug("remove: " + transactionId);
		return true;
	}
	
	public String get(int piece_number, String key) {

		synchronized (this) {
			
		}
		while (!readFlag.containsKey(piece_number)) {
			try {
				LOG.debug("sleep...");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		List<Map<String, String>> maps = readSet.get(piece_number);
		Map<String, String> map = maps.get(0);
		return map.get(key);

	}
	
	public List<Map<String ,String>> getAll(int piece_number) {
		
		while (!readFlag.containsKey(piece_number)) {
			try {
				LOG.debug("sleep...");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}		
		return readSet.get(piece_number);
	}
	
	public void addvalueInteger(String name, int value) throws TransactionException {
		assertState();
		List<String> names = new ArrayList<String>();
		names.add(name);
		List<String> values = new ArrayList<String>();
		values.add(String.valueOf(value));
		Vertex v = new Vertex(Action.ADDINTEGER);
		v.setName(names);
		v.setValue(values);
		piece.getVertexs().add(v);
	}
	
	public void addvalueDecimal(String name, float value) throws TransactionException {
		assertState();
		List<String> names = new ArrayList<String>();
		names.add(name);
		List<String> values = new ArrayList<String>();
		values.add(String.valueOf(value));
		Vertex v = new Vertex(Action.ADDDECIMAL);
		v.setName(names);
		v.setValue(values);
		piece.getVertexs().add(v);
	}
	
	public void read(String name) throws TransactionException {
		assertState();
		List<String> names = new ArrayList<String>();
		names.add(name);
		Vertex v = new Vertex(Action.READSELECT);
		v.setName(names);
		piece.getVertexs().add(v);
	}
	
	public void readSelect(List<String> names) throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.READSELECT);
		v.setName(names);
		piece.getVertexs().add(v);
	}
	
	
	public void write(List<String> names, List<String> values) throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.WRITE);
		v.setName(names);
		v.setValue(values);
		piece.getVertexs().add(v);
	}
	
	public void write(String name, String value) throws TransactionException {
		assertState();
		List<String> names = new ArrayList<String>(), values = new ArrayList<String>();
		names.add(name);
		values.add(value);
		Vertex v = new Vertex(Action.WRITE);
		v.setName(names);
		v.setValue(values);
		piece.getVertexs().add(v);
	}
	
	public void fetchOne(List<String> names) throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.FETCHONE);
		v.setName(names);
		piece.getVertexs().add(v);
	}
	
	public void fetchAll(List<String> names) throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.FETCHALL);
		v.setName(names);
		piece.getVertexs().add(v);
	}

	public void delete() throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.DELETE);
		piece.getVertexs().add(v);
	}
	
	//---------------------------------------------------------
	public String getTransactionId() {
		return transactionId;
	}

	public boolean isFinished() {
		return finished;
	}

	public void setFinished(boolean finished) {
		this.finished = finished;
	}

	private void assertState() throws TransactionException {
        if (this.transactionId == null) {
            throw new TransactionException("Read operation invoked before begin");
        } else if (this.finished) {
            throw new TransactionException("Attempted operation on completed transaction");
        }
    }

}
