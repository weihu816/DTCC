package cn.ict.rococo.procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import cn.ict.rococo.coordinator.messaging.CoordinatorCommunicator;
import cn.ict.rococo.messaging.Action;
import cn.ict.rococo.messaging.Piece;
import cn.ict.rococo.messaging.ReturnType;
import cn.ict.rococo.messaging.Vertex;

public class RococoTransaction {
	
	private static final Log log = LogFactory.getLog(RococoTransaction.class);
	
	protected String transactionId;
	protected boolean finished = false;
	private Piece piece = null;
	CoordinatorCommunicator communicator;
	int piece_number;
	private Map<Integer, HashMap<String, String>> readSet = new HashMap<Integer, HashMap<String, String>>();
	ExecutorService cachedThreadPool; 
	
	public RococoTransaction() {
		communicator = new CoordinatorCommunicator();
	}
	
	public void begin() {
		transactionId = UUID.randomUUID().toString();
		cachedThreadPool = Executors.newCachedThreadPool();
		piece_number = 0;
	}
	
	public boolean commit() throws TransactionException {
		if (piece != null) {
			throw new TransactionException("Commit a txn without complete the pieces");
		}
		this.finished = true;
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
		if (piece.isImmediate()) {
//			cachedThreadPool.execute(new Runnable() {
//				@Override
//				public void run() {
//					
//				}
//			}); 
			try {
				ReturnType returnType = communicator.fistRound(piece);
				HashMap<String, String> map = readSet.get(piece_number);
				if (map == null) {
					map = new HashMap<String, String>();
					readSet.put(piece_number, map);
				}
				map.putAll(returnType.getOutput());
			} catch (TException e) {
				e.printStackTrace();
			}
		}
		piece = null;
	}
	
	public String get(int piece_number, String key) throws TransactionException {
		String value;
		try {
			HashMap<String, String> map = readSet.get(piece_number);
			while (map == null || map.get(key) == null) {
				Thread.sleep(100);
				log.info("sleep...");
				map = readSet.get(piece_number);
			}
			value = map.get(key);
		} catch (Exception e) {
			throw new TransactionException("No such element from the read set");
		}
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

	
	public void readSelect(List<String> name) throws TransactionException {
		assertState();
		Vertex v = new Vertex(Action.READSELECT, name);
		piece.getVertexs().add(v);
	}
	
	
	public void write(List<String> names, List<String> values) throws TransactionException {
		assertState();
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
