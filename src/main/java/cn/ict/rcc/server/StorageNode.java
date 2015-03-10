package cn.ict.rcc.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;

import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Graph;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.config.ServerConfiguration;
import cn.ict.rcc.server.dao.MemoryDB;

public class StorageNode {

	private static final Log LOG = LogFactory.getLog(StorageNode.class);
	public static final int STARTED = 0;
	public static final int COMMITTING = 1;
	public static final int DECIDED = 2;

	private MemoryDB db = new MemoryDB();
	private ServerConfiguration configuration;
	private ServerCommunicator communicator;

	private ConcurrentMap<String, Integer> status = new ConcurrentHashMap<String, Integer>();

	private Map<String, String> dep_server = new ConcurrentHashMap<String, String>();

	private ConcurrentHashMap<String, List<String>> pieces_conflict = new ConcurrentHashMap<String, List<String>>();
	private ConcurrentHashMap<String, List<Piece>> pieces = new ConcurrentHashMap<String, List<Piece>>();

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
		LOG.debug("start_req(piece)");

		String id = piece.transactionId;
		String theKey = piece.getTable() + "_" + piece.getKey();

		Map<String, String> output = new HashMap<String, String>();

		/* S.dep[p.owner].status = STARTED */
		status.put(id, STARTED);

		// update - the most recent piece conflicted
		List<String> conflictPieces = pieces_conflict.get(theKey);
		if (piece.isImmediate() && conflictPieces != null && conflictPieces.size() > 0) {
			String p_id = conflictPieces.get(conflictPieces.size() - 1);
			if (p_id != piece.getTransactionId() && status.get(p_id) != DECIDED) {
				LOG.debug(piece.getTransactionId() + "<-" + p_id);
				dep_server.put(piece.getTransactionId(), p_id);
			}
		}
		if (piece.isImmediate()) {
			output = execute(piece);
			status.put(id, COMMITTING);
		}
		/* buffer piece */
		if (conflictPieces == null) {
			conflictPieces = Collections.synchronizedList(new ArrayList<String>());
			pieces_conflict.put(theKey, conflictPieces);
		}
		if (!piece.isImmediate()) {
			LOG.debug("Put txn: " + piece.transactionId);
			List<Piece> allPieces = pieces.get(piece.getTransactionId());
			if (allPieces == null) {
				allPieces = new ArrayList<Piece>();
				pieces.put(piece.transactionId, allPieces);
			}
			allPieces.add(piece);
		}
		conflictPieces.add(piece.transactionId);

		ReturnType returnType = new ReturnType();
		returnType.setOutput(output);
		Graph g = new Graph();
		Map<String, String> map = new HashMap<String, String>();
		map.putAll(dep_server);
		g.setVertexes(map);
		returnType.setDep(g);
		LOG.debug(returnType);
		return returnType;
	}

	public ReturnType commit_req(String transactionId, Graph dep) throws TException {
		LOG.debug("commit_req(String transactionId, Piece piece): txn " + transactionId);
		// s.dep union dep
		ReturnType returnType = new ReturnType();
		Map<String, String> output = new HashMap<String, String>();
		returnType.setOutput(output);
		
		if (status.get(transactionId) == DECIDED) {
			return returnType;
		} else if (status.get(transactionId) == COMMITTING) {
			status.put(transactionId, DECIDED);
			LOG.debug("commit_req DONE: txn " + transactionId);
			return returnType;
		}
		
		synchronized (this) {
			String v = transactionId;
			Stack<String> stack = new Stack<String>();
			stack.push(transactionId);
			while ((v = dep.vertexes.get(v)) != null) {
				if (status.get(v) != null && status.get(v) == DECIDED) {
					break;
				}
				stack.push(v);
				LOG.debug("commit_req: push txn " + v);
			}
			
			while (!stack.isEmpty() && (v = stack.pop()) != null) {
				while (status.get(v) == null) {
					try {
						Thread.currentThread().wait(1000);
						LOG.debug("wait for " + v);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} 
				if (status.get(v) == STARTED)  {
					status.put(v, DECIDED);
					LOG.debug("execute " + v);
					for (Piece p : pieces.get(v)) {
						output.putAll(execute(p));
					}
				}
			}
			status.put(transactionId, DECIDED);
			LOG.debug("output: " + output);
			LOG.debug("commit_req DONE: txn " + transactionId);
			return returnType;
		}
	}

	public synchronized Map<String, String> execute(Piece piece)
			throws TException {
		LOG.debug("execute(Piece piece)");
		Map<String, String> output = new HashMap<String, String>();
		List<Vertex> vertexs = piece.getVertexs();
		for (Vertex v : vertexs) {
			Action action = v.getAction();
			String table = piece.getTable();
			String key = piece.getKey();
			List<String> names = v.getName();
			List<String> values = v.getValue();
			switch (action) {
			case READSELECT:
				Map<String, String> m = db.read(table, key, names);
				if (m != null) {
					output.putAll(m);
				}
				LOG.debug("Read - Table:" + table + " Key:" + key + " names: " + names);
				break;
			case WRITE:
				if (!db.write(table, key, names, values)) {
					LOG.debug("Error Write - ");
				}
				LOG.debug("Insert - Table:" + table + " Key:" + key + " names: " + names);
				break;
			case ADDVALUE:
				if (!db.addInteger(table, key, names, values)) {
					LOG.debug("Error Write - ");
				}
				LOG.debug("Addvalue - Table:" + table + " Key:" + key + " names: " + names);
				break;
			default:
			}
		}
		return output;
	}

	public synchronized boolean write(String table, String key,
			List<String> names, List<String> values) {
//		LOG.debug("write: " + table + " " + key);
		return db.write(table, key, names, values);
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
