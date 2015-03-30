package cn.ict.rcc.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;

import cn.ict.dtcc.config.ServerConfiguration;
import cn.ict.dtcc.util.RccUtil;
import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.dao.MemoryDB;

public class StorageNode {

	private static final Log LOG = LogFactory.getLog(StorageNode.class);
	public static final int STARTED = 0;
	public static final int COMMITTING = 1;
	public static final int DECIDED = 2;

	ConcurrentMap<String, Integer> status = new ConcurrentHashMap<String, Integer>();

	private MemoryDB db = new MemoryDB();
	private ServerConfiguration configuration;
	private ServerCommunicator communicator;
	private Map<String, String> dep_server = new ConcurrentHashMap<String, String>();

	private ConcurrentHashMap<String, List<String>> pieces_conflict = new ConcurrentHashMap<String, List<String>>();
	private ConcurrentHashMap<String, Set<Piece>> pieces = new ConcurrentHashMap<String, Set<Piece>>();

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

	// ---------------------------------------------------------------
	public synchronized ReturnType start_req(Piece piece) throws TException {
		LOG.debug("start_req (piece) txn: " + piece.transactionId + " " + piece.table + " " + piece.key);

		if (status.get(piece.transactionId) == null) {
			status.put(piece.transactionId, STARTED);
		}
		String theKey = RccUtil.buildString(piece.getTable(), "_", piece.getKey());
		
		List<Map<String, String>> output = new ArrayList<Map<String, String>>();

		// update - the most recent piece conflicted
		List<String> conflictPieces = pieces_conflict.get(theKey);
		if (piece.isImmediate() && conflictPieces != null && conflictPieces.size() > 0) {
			String p_id = conflictPieces.get(conflictPieces.size() - 1);
			if (!p_id.equals(piece.getTransactionId()) && status.get(p_id) != DECIDED) {
				LOG.info(piece.getTransactionId() + "<-" + p_id);
				dep_server.put(piece.getTransactionId(), p_id);
			}
		}
		if (piece.isImmediate()) {
			output = execute(piece);
		}
		/* buffer piece */
		if (conflictPieces == null) {
			conflictPieces = Collections.synchronizedList(new ArrayList<String>());
			pieces_conflict.put(theKey, conflictPieces);
		}
		if (!piece.isImmediate()) {
			LOG.debug("Put txn: " + piece.transactionId + " " + piece.table + " " + piece.key);
			Set<Piece> allPieces = pieces.get(piece.getTransactionId());
			if (allPieces == null) {
				allPieces = new ConcurrentSkipListSet<Piece>();
				pieces.put(piece.transactionId, allPieces);
			}
			allPieces.add(piece);
		}
		conflictPieces.add(piece.transactionId);

		ReturnType returnType = new ReturnType();
		returnType.setOutput(output);
		Graph g = new Graph();
		Map<String, String> map = new HashMap<String, String>(dep_server);
		g.setVertexes(map);
		returnType.setDep(g);
		LOG.debug(returnType);
		return returnType;
	}

	public synchronized boolean commit_req(String transactionId, Graph dep) throws TException {
			
		LOG.debug("commit_req (piece) txn: " + transactionId + " " + dep);

		if (status.get(transactionId) == DECIDED) {
			LOG.debug("commit_req already done *DONE: txn " + transactionId);
			return true;
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
			//if txn v does not involve S, we should not wait for the arrival of that txn
//			if (pieces.get(v) == null) {
//				continue;
//			}
			while (!status.containsKey(v)) {
				LOG.debug("waiting1: " + v);
				throw new RuntimeException("1");
			}
			while (status.get(v) == STARTED) {
				try {
					Thread.sleep(10);
					LOG.debug("waiting2: " + v);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			LOG.debug("execute " + v);
			if (pieces.get(v) != null) {
				for (Piece p : pieces.get(v)) {
					execute(p);
					// remove the buffed piece
					pieces.get(v).remove(p);
					// remove the piece from conflict information
					String theKey = RccUtil.buildString(p.getTable(), "_",
							p.getKey());
					List<String> conflictPieces = pieces_conflict.get(theKey);
					conflictPieces.remove(p.getTransactionId());
					if (dep_server.containsKey(p.getTransactionId())) {
						dep_server.remove(p.getTransactionId());
						LOG.info("remove: " + p.getTransactionId());
					}
				}
			}
			if (dep_server.containsKey(v)) {
				dep_server.remove(v);
				LOG.info("remove: " + v);
			}
			status.put(v, DECIDED);
		}

		LOG.debug("commit_req DONE: txn " + transactionId);
		LOG.debug("status:  " + status.get(transactionId));

		pieces.remove(transactionId);
		return true;
	}

	public synchronized List<Map<String, String>> execute(Piece piece)
			throws TException {
		LOG.debug("execute(Piece piece) " + piece.table + " " + piece.key);
		List<Map<String, String>> output = new ArrayList<Map<String, String>>();
		List<Vertex> vertexs = piece.getVertexs();
		for (Vertex v : vertexs) {
			Action action = v.getAction();
			String table = piece.getTable();
			String key = piece.getKey();
			List<String> names = v.getName();
			List<String> values = v.getValue();
			List<Map<String, String>> m = null;
			switch (action) {
			case READSELECT:
				LOG.debug("Read - Table:" + table + " Key:" + key + " names: " + names);
				Map<String, String> tmp = db.read(table, key, names);
				if (tmp != null) {
					output.add(tmp);
				}
				break;
			case FETCHONE:
				LOG.debug("Fetch - Table:" + table + " Key:" + key + " names: " + names);
				m = db.read_secondaryIndex(table, key, names, false);
				if (m != null) {
					output.addAll(m);
				}
				break;
			case FETCHALL:
				LOG.debug("Fetch - Table:" + table + " Key:" + key + " names: " + names);
				m = db.read_secondaryIndex(table, key, names, true);
				if (m != null) {
					output.addAll(m);
				}
				break;
			case WRITE:
				LOG.debug("Insert - Table:" + table + " Key:" + key + " names: " + names);
				if (!db.write(table, key, names, values)) {
					LOG.debug("Error Write - ");
				}
				break;
			case ADDINTEGER:
				LOG.debug("Addvalue - Table:" + table + " Key:" + key + " names: " + names + " values: " + names);
				if (!db.add(table, key, names, values, false)) {
					LOG.debug("Error Write - ");
				}
				break;
			case ADDDECIMAL:
				LOG.debug("Addvalue - Table:" + table + " Key:" + key + " names: " + names + " values: " + names);
				if (!db.add(table, key, names, values, true)) {
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

	public synchronized boolean write(String table, String key,
			List<String> names, List<String> values) {
		return db.write(table, key, names, values);
	}
	
	public boolean createSecondaryIndex(String table, List<String> fields) {
		return db.createSecondaryIndex(table, fields);
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(ServerConfiguration.getConfiguration()
				.getLogConfigFilePath());
		final StorageNode storageNode = new StorageNode();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				storageNode.stop();
			}
		});
		storageNode.start();
	}

}
