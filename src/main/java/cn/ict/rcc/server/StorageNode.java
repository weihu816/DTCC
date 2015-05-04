package cn.ict.rcc.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;

import cn.ict.dtcc.benchmark.tpcc.TPCCConstants;
import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.dtcc.server.dao.MemoryDB;
import cn.ict.dtcc.util.DTCCUtil;
import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.CommitResponse;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.StartResponseBulk;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.coordinator.messaging.AskListener;
import cn.ict.rcc.server.coordinator.messaging.CoordinatorCommunicator;

public class StorageNode {

	private static final Log LOG = LogFactory.getLog(StorageNode.class);
	
	public static final int STARTED 	= 0;
	public static final int COMMITTING 	= 1;
	public static final int DECIDED 	= 2;

	private MemoryDB db = new MemoryDB();		// dao
	private ServerConfiguration configuration; 	// configuration and membership
												// information
	private ServerCommunicator communicator; 	// service listener
	// communicate with other servers
	private CoordinatorCommunicator coorCommunicator = new CoordinatorCommunicator();

	private Map<String, String> dep_server = new ConcurrentHashMap<String, String>();
	
	// all txn record will be kept in case asked by other servers
	ConcurrentMap<String, Integer> status = new ConcurrentHashMap<String, Integer>();

	ConcurrentMap<String, Set<String>> serversInvolvedList = new ConcurrentHashMap<String, Set<String>>();

	private ConcurrentHashMap<String, List<String>> pieces_conflict = new ConcurrentHashMap<String, List<String>>();
	private ConcurrentHashMap<String, Set<Piece>> pieces = new ConcurrentHashMap<String, Set<Piece>>();

	private ConcurrentHashMap<String, Map<Integer, List<String>>> bufferOutput = new ConcurrentHashMap<String, Map<Integer, List<String>>>();

	private Random rand = new Random(System.nanoTime());
	private void randomBackoff() {
		try {
			long sleepTime = (long) (rand.nextInt(10000) % 300);
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public StorageNode() {
		this.configuration = ServerConfiguration.getConfiguration();
		this.communicator = new ServerCommunicator();
	}

	public void start() {
		db.init();
		int port = configuration.getLocalMember().getPort();
		communicator.startListener(this, port);
	}

	public void stop() {
		communicator.stopListener();
	}

	/*
	 * First phase of RCC Build the temporary order
	 */
	public synchronized StartResponseBulk start_req(List<Piece> ps) throws TException {

		StartResponseBulk startResponseBulk = new StartResponseBulk();
		List<List<String>> outputsList = new ArrayList<List<String>>();

		for (Piece piece : ps) {
			
			if (status.get(piece.transactionId) == null) { status.put(piece.transactionId, STARTED); }

			String table = piece.table;
			String key = piece.getKey();
			String theKey = DTCCUtil.buildKey(table, key);
			List<String> output = new ArrayList<String>();

			LOG.debug("start_req (piece) txn: " + piece.transactionId + " " + table + " " + key);

			List<String> conflictPieces = pieces_conflict.get(theKey);
			if (!piece.getTable().equals("STOCK") && !piece.getTable().equals("ITEM")) {
				// update - the most recent piece conflicted
				if (piece.isImmediate() && conflictPieces != null && conflictPieces.size() > 0) {
					String p_id = conflictPieces.get(conflictPieces.size() - 1);
					if (!p_id.equals(piece.getTransactionId()) && status.get(p_id) != DECIDED) {
						LOG.info(piece.getTransactionId() + "<-" + p_id);
						dep_server.put(piece.getTransactionId(), p_id);
					}
				}
			}
			if (conflictPieces == null) {
				conflictPieces = new ArrayList<String>();
				pieces_conflict.put(theKey, conflictPieces); // Collections.synchronizedList()
			}
			if (piece.isImmediate()) {
				output = execute(piece);
			} else {
				LOG.debug("Put txn: " + piece.transactionId + " " + table + " " + key);
				Set<Piece> allPieces = pieces.get(piece.getTransactionId());
				if (allPieces == null) {
					allPieces = new ConcurrentSkipListSet<Piece>();
					pieces.put(piece.transactionId, allPieces);
				}
				allPieces.add(piece);
			}
			conflictPieces.add(piece.transactionId);
			outputsList.add(output);
		}

		startResponseBulk.setOutput(outputsList);
		Graph g = new Graph();
		g.setVertexes(new HashMap<String, String>(dep_server));
		startResponseBulk.setDep(g);
		return startResponseBulk;
	}

	/*
	 * Second phase of RCC Tract dependency information and execution deferrable
	 * pieces
	 */
	public synchronized CommitResponse commit_req(String transactionId, Graph dep) throws TException {

		LOG.debug("commit_req (piece) txn: " + transactionId + " " + dep);
		serversInvolvedList.putAll(dep.getServersInvolved());

		if (status.get(transactionId) == DECIDED) {
			LOG.debug("commit already  *** DONE: txn " + transactionId);
			CommitResponse commitResponse = new CommitResponse(true);
			Map<Integer, List<String>> txnOutput = new HashMap<Integer, List<String>>(bufferOutput.get(transactionId));
			commitResponse.setOutput(txnOutput);
			bufferOutput.remove(transactionId);
			LOG.debug("commit return: " + txnOutput);
			return commitResponse;
		}

		String v = transactionId;
		Stack<String> stack = new Stack<String>();
		stack.push(transactionId);
		while ((v = dep.vertexes.get(v)) != null) {
			if (status.containsKey(v) && status.get(v) == DECIDED) {
				break;
			}
			stack.push(v);
			LOG.debug("commit_req: push txn " + v);
		}

		while (!stack.isEmpty() && (v = stack.pop()) != null) {
			if (!status.containsKey(v)) {
				// if txn v does not involve S, we should not wait for the arrival of that txn
				// we need to communicate with other servers
				boolean toCon = true;
				while (toCon) {
					LOG.debug("NULL!!! Haven't arrived or not involved ???");
					for (String s : serversInvolvedList.get(v)) {
						AskListener askListener = new AskListener(configuration.getMember(s), coorCommunicator, v);
						askListener.start();
						synchronized (askListener) {
							long start = System.currentTimeMillis();
							LOG.warn("Asking  txn " + v + " for commit");
							while (askListener.getResult() == 0) {
								try {
									askListener.wait(5000);
								} catch (InterruptedException ignored) { }
								finally { LOG.debug("Ask Response: " + askListener.getResult()); }
								if (System.currentTimeMillis() - start > 10000) {
									LOG.warn("Asking txn " + v + " timed out");
									break;
								}
							}
							if (askListener.getResult() == 1) {
								toCon = false;
								break;
							}
						}
					}
				}
			}
			
			// txn v is committing and this sever does not involve txn v
			if (!status.containsKey(v)) { continue; } 
			
			// txn involves server and wait for txn to be committing
			while (status.get(v) == STARTED) {
				randomBackoff();
				LOG.debug("waiting2: " + v);
			}

			LOG.debug("execute txn" + v);
			if (bufferOutput.get(v) == null) { 
				bufferOutput.put(v, new HashMap<Integer, List<String>>());
			}

			if (pieces.get(v) != null) {
				Map<Integer, List<String>> map = bufferOutput.get(v);
				for (Piece p : pieces.get(v)) {
					List<String> output = execute(p);
					map.put(p.getId(), output);
					// remove the buffed piece
					pieces.get(v).remove(p);
					// remove the piece from conflict information
					String theKey = DTCCUtil.buildString(p.getTable(), "_", p.getKey());
					List<String> conflictPieces = pieces_conflict.get(theKey);
					conflictPieces.remove(p.getTransactionId());
					if (dep_server.get(p.getTransactionId()) != null) {
						dep_server.remove(p.getTransactionId());
						LOG.info("remove: " + p.getTransactionId());
					}
				}
			}
			if (dep_server.get(v) != null) {
				dep_server.remove(v);
				LOG.info("remove: " + v);
			}
			status.put(v, DECIDED);
		}

		LOG.debug("commit_req DONE: txn " + transactionId);
		LOG.debug("status:  " + status.get(transactionId));
		pieces.remove(transactionId);

		CommitResponse commitResponse = new CommitResponse(true);
		Map<Integer, List<String>> txnOutput = new HashMap<Integer, List<String>>();
		txnOutput.putAll(bufferOutput.get(transactionId));
		commitResponse.setOutput(txnOutput);
		bufferOutput.remove(transactionId);
		LOG.debug("commit return: " + txnOutput);
		return commitResponse;
	}

	/*
	 * second phase of RCC
	 */
	public synchronized List<String> execute(Piece piece) throws TException { // List<Map<String, String>>
		LOG.debug("execute(Piece piece) " + piece.table + " " + piece.key);
		List<String> output = new ArrayList<String>();
		List<Vertex> vertexs = piece.getVertexs();
		for (Vertex v : vertexs) {
			Action action = v.getAction();
			String table = piece.getTable();
			String key = piece.getKey();
			List<String> names = v.getName();
			List<String> values = v.getValue();
			switch (action) {
			case READSELECT:
				LOG.debug("Read - Table:" + table + " Key:" + key + " names: " + names);
				List<String> list = db.read(table, key, names);
				if (list != null) { output.addAll(list); }
				break;
//			case FETCHONE:
//				LOG.debug("Fetch - Table:" + table + " Key:" + key + " names: " + names);
//				m = db.read_secondaryIndex(table, key, names, false);
//				if (m != null) {
//					output.addAll(m);
//				}
//				break;
//			case FETCHALL:
//				LOG.debug("Fetch - Table:" + table + " Key:" + key + " names: " + names);
//				m = db.read_secondaryIndex(table, key, names, true);
//				if (m != null) {
//					output.addAll(m);
//				}
//				break;
			case WRITE:
				LOG.debug("Insert - Table:" + table + " Key:" + key + " names: " + names);
				if (!db.write(table, key, names, values)) {
					LOG.debug("Error Write - ");
				}
				break;
			case ADDI:
				LOG.debug("Addvalue - Table:" + table + " Key:" + key + " names: " + names + " values: " + names);
				if (!db.add(table, key, names, values, false)) {
					LOG.debug("Error Write - ");
				}
				break;
			case ADDF:
				LOG.debug("Addvalue - Table:" + table + " Key:" + key + " names: " + names + " values: " + names);
				if (!db.add(table, key, names, values, true)) {
					LOG.debug("Error Write - ");
				}
				break;
			case REDUCEI:
				LOG.debug("ReduceValue - Table:" + table + " Key:" + key + " names: " + names + " values: " + names);
				if (!db.reduce(table, key, names, values)) {
					LOG.debug("Error Write - ");
				}
				break;
			case DELETE:
				LOG.debug("Delete - Table:" + table + " Key:");
				if (!db.delete(table, key)) {
					LOG.debug("Error Delete - ");
				}
				break;
			default:
			}
		}
		return output;
	}

	/*
	 * Used sed by database init only
	 */
	public synchronized boolean write(String table, String key,
			List<String> names, List<String> values) {
		return db.write(table, key, names, values);
	}

	/*
	 * Create Index fields other than key
	 */
	public boolean createSecondaryIndex(String table, List<String> fields) {
		return db.createSecondaryIndex(table, fields);
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(ServerConfiguration.getConfiguration().getLogConfigFilePath());
		final StorageNode storageNode = new StorageNode();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				storageNode.stop();
			}
		});
		storageNode.start();
	}

	public boolean rcc_ask_txnCommitting(String transactionId) {
		LOG.info("Asking about txn" + transactionId);
		if (status.get(transactionId) == null) {
			throw new RuntimeException("Wrong transactionId when asking");
		}
		if (status.get(transactionId) >= COMMITTING) {
			LOG.debug("Response true !");
			return true;
		} else {
			LOG.info("randomBackoff()");
			randomBackoff(); // random back off waiting for txn to be committing
		}
		return (status.get(transactionId) >= COMMITTING);
	}
}
