package cn.ict.rcc.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;

import cn.ict.rcc.messaging.Action;
import cn.ict.rcc.messaging.Edge;
import cn.ict.rcc.messaging.Piece;
import cn.ict.rcc.messaging.ReturnType;
import cn.ict.rcc.messaging.Vertex;
import cn.ict.rcc.server.config.ServerConfiguration;
import cn.ict.rcc.server.dao.MemoryDB;

public class StorageNode {

	private static final Log LOG = LogFactory.getLog(StorageNode.class);

	public static final int STARTED 	= 0;
	public static final int COMMITTING 	= 1;
	public static final int DECIDED 	= 2;

	private MemoryDB db = new MemoryDB();
	private ServerConfiguration configuration;
	private ServerCommunicator communicator;

	private ConcurrentMap<String, Integer> status = new ConcurrentHashMap<String, Integer>();
	
	private Set<Edge> dep_server = new ConcurrentSkipListSet<Edge>();

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
	public ReturnType start_req(Piece piece) throws TException {
		LOG.info("start_req(piece)");
		
		String id = piece.transactionId;
		String theKey = piece.getTable() + "_" + piece.getKey();
		
		Map<String, String> output = new HashMap<String, String>();
		
		/* S.dep[p.owner].status = STARTED */
		status.put(id, STARTED);
		
		/* foreach p' received by Server that conflicts with p */
		List<Piece> conflictPieces = pieces.get(theKey);
		if (conflictPieces != null && conflictPieces.size() > 0) {
			for (Piece p : conflictPieces) {
				if (p.getTransactionId() != piece.getTransactionId()) {
					Edge edge = new Edge();
					edge.setFrom(p.getTransactionId()); // 什么时候移除
					edge.setTo(piece.transactionId);
					edge.setImmediate(piece.isImmediate());
					dep_server.add(edge);
				}
			}
		}

		if (piece.isImmediate()) {
			output = execute(piece);
		}
		/* buffer piece */
		if (conflictPieces == null) {
			conflictPieces = Collections.synchronizedList(new ArrayList<Piece>());
			pieces.put(theKey, conflictPieces);
		}
		conflictPieces.add(piece);

		ReturnType returnType = new ReturnType();
		returnType.setOutput(output);
		returnType.setEdges(new ConcurrentSkipListSet<Edge>(dep_server));
		LOG.info(returnType);
		return returnType;
	}

	public ReturnType commit_req(String transactionId, Piece piece, Set<Edge> dep) {
		LOG.info("commit_req(String transactionId, Piece piece)");
		// s.dep union dep
		Set<Edge> dep_union = new ConcurrentSkipListSet<Edge>(dep);
		dep_union.addAll(dep_server);
		
		status.put(transactionId, COMMITTING);
		
		
		return null;
	}

	public synchronized Map<String, String> execute(Piece piece)
			throws TException {
		LOG.info("execute(Piece piece)");
		Map<String, String> output = new HashMap<String, String>();
		List<Vertex> vertexs = piece.getVertexs();
		for (Vertex v : vertexs) {
			Action action = v.getAction();
			String table  = piece.getTable();
			String key 	  = piece.getKey();
			List<String> names  = v.getName();
			List<String> values = v.getValue();
			switch (action) {
			case READSELECT:
				Map<String, String> m = db.read(table, key, names);
				if (m != null) {
					output.putAll(m);
				}
				LOG.info("Read - Table:" + table + " Key:" + key);
				break;
			case WRITE:
				if (!db.write(table, key, names, values)) {
					System.err.println("ERROR WRITE");
					LOG.info("Error Write - ");
				}
				LOG.info("Insert - Table:" + table + " Key:" + key);
				break;
			case ADDVALUE:
				if (!db.addInteger(table, key, names, values)) {
					System.err.println("ERROR ADDVALUE");
					LOG.info("Error Write - ");
				}
				LOG.info("Addvalue - Table:" + table + " Key:" + key);
				break;
			default:
			}
		}
		return output;
	}

	public synchronized boolean write(String table, String key, List<String> names,
			List<String> values) {
		LOG.info("write: "  + table + " " + key);
		return db.write(table, key, names, values);
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

	
}
