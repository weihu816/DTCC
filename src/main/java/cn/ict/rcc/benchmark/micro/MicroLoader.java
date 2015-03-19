package cn.ict.rcc.benchmark.micro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TTransport;

import cn.ict.rcc.Member;
import cn.ict.rcc.benchmark.Procedure;
import cn.ict.rcc.benchmark.tpcc.TPCCConstants;
import cn.ict.rcc.benchmark.tpcc.TPCCGenerator;
import cn.ict.rcc.messaging.RococoCommunicationService;
import cn.ict.rcc.messaging.ThriftConnectionPool;
import cn.ict.rcc.server.config.ServerConfiguration;
import cn.ict.rcc.server.coordinator.txn.CoordinatorClient;

/**
 * The loader function for initial state of micro benchmark
 * @author Wei Hu
 */
public class MicroLoader {

	private static final Log LOG = LogFactory.getLog(MicroLoader.class);
	private KeyedObjectPool<Member, TTransport> blockingPool = new 
			StackKeyedObjectPool<Member, TTransport>(new ThriftConnectionPool());
	private HashMap<Member, RococoCommunicationService.Client> clientPool = new HashMap<Member, RococoCommunicationService.Client>();
	private ServerConfiguration config;
	
	
	public MicroLoader() { 
		config = ServerConfiguration.getConfiguration();
	}
	
	public void load() {
		List<String> names = new ArrayList<String>(), values = new ArrayList<String>();
		names.add("myValue");
		values.add("0");
		write("table1", "myKey", names, values);
		write("table2", "myKey", names, values);
	}

	private boolean write(String table, String key, List<String> names, List<String> values) {
		Member member = null;
		TTransport transport = null;
		try {
			member = config.getShardMember(table, key);
			RococoCommunicationService.Client client = clientPool.get(member);
			if (client == null) {
				transport = blockingPool.borrowObject(member);
				client = new RococoCommunicationService.Client(new TBinaryProtocol(transport));
				clientPool.put(member, client);
			}
			return client.write(table, key, names, values);
		} catch (Exception e) {
			if (member != null) {
				String msg = "Error contacting the remote member: " + member.getHostName();
				LOG.warn(msg, e);
			}
			return false;
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(ServerConfiguration.getConfiguration().getLogConfigFilePath());
		MicroLoader loader = new MicroLoader();
		loader.load();
	}
}