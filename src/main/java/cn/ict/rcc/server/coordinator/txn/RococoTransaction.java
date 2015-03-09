package cn.ict.rcc.server.coordinator.txn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Node;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorCommunicator;
import cn.ict.rcc.server.coordinator.messaging.MethodCallback;

public class RococoTransaction {
	
	public static AtomicInteger transactionIdGen = new AtomicInteger(0);
	
	private static final Log LOG = LogFactory.getLog(RococoTransaction.class);
	
	protected String transactionId;
	protected boolean finished = false;
	private Piece piece = null, tempPiece = null;
	private List<Piece> pieces = new ArrayList<Piece>();
	CoordinatorCommunicator communicator;
	int piece_number;
	private Map<Integer, HashMap<String, String>> readSet = new ConcurrentHashMap<Integer, HashMap<String, String>>();
	ExecutorService cachedThreadPool;
//	private Set<Edge> dep = new ConcurrentSkipListSet<Edge>();
//	private RccGraph dep = new RccGraph();
	Map<String, String> dep = new ConcurrentHashMap<String, String>();
	
	public RococoTransaction() {
		communicator = new CoordinatorCommunicator();
	}
	
	public void begin() {
//		transactionId = UUID.randomUUID().toString();
		transactionId = String.valueOf(transactionIdGen.addAndGet(1));
		piece_number = 0;
		cachedThreadPool = Executors.newCachedThreadPool();
	}
	
	public boolean commit() throws TransactionException {
		if (piece != null) {
			throw new TransactionException("Commit a txn without complete the pieces");
		}
		this.finished = true;
		try {
			Graph graph = new Graph();
			graph.setVertexes(dep);
			if (communicator.secondRound(transactionId, pieces, graph)) {
				// TODO: clean up
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		cachedThreadPool.shutdown();
		return true;
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
//		if (piece.isImmediate()) {
			tempPiece = piece;
			try {
				MethodCallback callback = communicator.fistRound(tempPiece);
				HashMap<String, String> map = readSet.get(piece_number);
				if (map == null) {
					map = new HashMap<String, String>();
					readSet.put(piece_number, map);
				}
				ReturnType returnType = callback.getResult();
				// update the dependency information
				map.putAll(returnType.getOutput());
				dep.putAll(returnType.getDep().getVertexes());
//				for(Entry<String, String> entry : returnType.getDep().getVertexes().entrySet()) {
//					if (dep.containsKey(entry.getKey())) {
//						dep.get(entry.getKey()).addAll(entry.getValue());
//					} else {
//						dep.put(entry.getKey(), entry.getValue());
//					}
//				}
				LOG.info(returnType);
				LOG.info(dep);
			} catch (TException e) {
				e.printStackTrace();
			}
//		}
		piece = null;
	}
	
	public String get(int piece_number, String key) {
		String value;

		HashMap<String, String> map = readSet.get(piece_number);
		while (map == null || map.get(key) == null) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			LOG.info("sleep...");
			map = readSet.get(piece_number);
		}
		value = map.get(key);

		return value;
	}
	
	public void addvalue(String name, int value) throws TransactionException {
		assertState();
		List<String> names = new ArrayList<String>();
		names.add(name);
		List<String> values = new ArrayList<String>();
		values.add(String.valueOf(value));
		Vertex v = new Vertex(Action.ADDVALUE, names);
		v.setValue(values);
		piece.getVertexs().add(v);
	}
	
	public void reducevalue(String name, int value) {
		List<String> names = new ArrayList<String>();
		names.add(name);
		List<String> values = new ArrayList<String>();
		values.add(String.valueOf(value));
		Vertex v = new Vertex(Action.REDUCEVALUE, names);
		v.setValue(values);
		piece.getVertexs().add(v);
	}

	
	public void read(String name) throws TransactionException {
		assertState();
		List<String> names = new ArrayList<String>();
		names.add(name);
		Vertex v = new Vertex(Action.READSELECT, names);
		piece.getVertexs().add(v);
	}
	
	public void readSelect(List<String> names) throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.READSELECT, names);
		piece.getVertexs().add(v);
	}
	
	
	public void write(List<String> names, List<String> values) throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.WRITE, names);
		v.setValue(values);
		piece.getVertexs().add(v);
	}
	
	public void write(String name, String value) throws TransactionException {
		assertState();
		List<String> names = new ArrayList<String>(), values = new ArrayList<String>();
		names.add(name);
		values.add(value);
		Vertex v = new Vertex(Action.WRITE, names);
		v.setValue(values);
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
