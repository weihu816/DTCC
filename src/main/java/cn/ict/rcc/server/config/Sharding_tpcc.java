package cn.ict.rcc.server.config;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.rcc.Member;
import cn.ict.rcc.benchmark.tpcc.TPCCConstants;
import cn.ict.rcc.benchmark.tpcc.TPCCScaleParameters;

public class Sharding_tpcc {
	private static final Log LOG = LogFactory.getLog(Sharding_tpcc.class);

	public static Member getShardMember(Map<Integer,Member[]> members, String table, String key) {
		int shardId, procId, index;
		switch (table) {
		// shard according to w_id and d_id
		case TPCCConstants.TABLENAME_DISTRICT:
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.lastIndexOf("_") + 1));
			index = (procId - 1) / TPCCConstants.DISTRICTS_PER_NODE;
			return members.get(shardId - 1)[index];
			
			
		case TPCCConstants.TABLENAME_CUSTOMER: // w_d_?
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.indexOf("_") + 1, key.lastIndexOf("_")));
			index = (procId - 1) / TPCCConstants.DISTRICTS_PER_NODE;
			return members.get(shardId - 1)[index];
	
		case TPCCConstants.TABLENAME_NEW_ORDER:
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			if (key.lastIndexOf("_") != key.indexOf("_")) {
				procId = Integer.parseInt(key.substring(key.indexOf("_") + 1, key.lastIndexOf("_")));
			} else {
				procId = Integer.parseInt(key.substring(key.indexOf("_") + 1));
			}
			index = (procId - 1) / TPCCConstants.DISTRICTS_PER_NODE;
			return members.get(shardId - 1)[index];
		case TPCCConstants.TABLENAME_ORDER:
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.indexOf("_") + 1, key.lastIndexOf("_")));
			index = (procId - 1) / TPCCConstants.DISTRICTS_PER_NODE;
			return members.get(shardId - 1)[index];
			
			
		case TPCCConstants.TABLENAME_ORDER_LINE:
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.indexOf("_") + 1, key.indexOf("_", key.indexOf("_") + 1)));			
			index = (procId - 1) / TPCCConstants.DISTRICTS_PER_NODE;
			return members.get(shardId - 1)[index];
			
			
		case TPCCConstants.TABLENAME_WAREHOUSE:
			shardId = Integer.parseInt(key);
			return members.get(shardId-1)[0];
			
			
		case TPCCConstants.TABLENAME_ITEM:
			return members.get(0)[0];
		case TPCCConstants.TABLENAME_HISTORY:
			return members.get(0)[0];
			
			
		case TPCCConstants.TABLENAME_STOCK: // w_i
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.indexOf("_") + 1));
			index = (procId - 1) % TPCCConstants.NODES_PER_WAREHOUSE;
			return members.get(shardId - 1)[index];
		default:
			break;
		}
		return null;
	}

}
