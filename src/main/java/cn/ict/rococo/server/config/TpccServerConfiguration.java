package cn.ict.rococo.server.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.MembershipKey;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.rococo.Member;
import cn.ict.rococo.benchmark.tpcc.TPCCConstants;
import cn.ict.rococo.benchmark.tpcc.TPCCScaleParameters;
import cn.ict.rococo.exception.RococoException;

/**
 * Reads Server Configuration Information
 * @author Wei Hu
 *
 */
public class TpccServerConfiguration {
		
	private static final Log LOG = LogFactory.getLog(TpccServerConfiguration.class);
	
	private static volatile TpccServerConfiguration config = null;
	
	private int myShardId = 0;
	private int myProcessId = 0;
	private Map<Integer,Member[]> members = new HashMap<Integer, Member[]>();
	private String logConfigfile = "conf/log4j-server.properties";
	
	private TpccServerConfiguration(Properties properties) {
		
		logConfigfile = properties.getProperty("logConfigfile", "conf/log4j-server.properties");
		
		String shardIdValue = System.getProperty("rococo.tpccShardId", properties.getProperty("rococo.shardId"));;
		String processIdValue = System.getProperty("rococo.tpccProcessId", properties.getProperty("rococo.processId"));
		if (shardIdValue != null && processIdValue != null) {
			myShardId = Integer.parseInt(shardIdValue);
			myProcessId = Integer.parseInt(processIdValue);
		}

		Map<Integer,List<Member>> tempMembers = new HashMap<Integer,List<Member>>();
        for (String property : properties.stringPropertyNames()) {
            if (property.startsWith("rococo.server")) {
                int shardId = Integer.parseInt(property.substring(14, property.indexOf('.', 15)));
                String value = properties.getProperty(property);
                String processId = property.substring(property.lastIndexOf('.') + 1);
                String[] connection = value.split(":");
                Member member = new Member(connection[0], Integer.parseInt(connection[1]), processId, false);
                List<Member> temp = tempMembers.get(shardId);
                if (temp == null) {
                    temp = new ArrayList<Member>();
                    tempMembers.put(shardId, temp);
                }
                temp.add(member);
            }
        }
        for (Map.Entry<Integer,List<Member>> entry : tempMembers.entrySet()) {
            Collections.sort(entry.getValue(), new Comparator<Member>() {
                public int compare(Member o1, Member o2) {
                    try {
                        int o1Id = Integer.parseInt(o1.getProcessId());
                        int o2Id = Integer.parseInt(o2.getProcessId());
                        return o1Id - o2Id;
                    } catch (NumberFormatException e) {
                        return o1.getProcessId().compareTo(o2.getProcessId());
                    }
                }
            });
            members.put(entry.getKey(), entry.getValue().toArray(new Member[entry.getValue().size()]));
        }
	}
	
	public static TpccServerConfiguration getConfiguration() {
		if (config == null) {
			synchronized (TpccServerConfiguration.class) {
				if (config == null) {
					String configPath = System.getProperty("server.config.dir", "conf");
					Properties props = new Properties();
					File configFile = new File(configPath, "tpccServer.properties");
					try {
						props.load(new FileInputStream(configFile));
						config = new TpccServerConfiguration(props);
					} catch (IOException e) {
						String msg = "Error loading Rococo configuration from: " + configFile.getPath();
						LOG.error(msg, e);
						throw new RococoException(msg, e);
					}
				}
			}
		}
		return config;
	}
	
	public Member[] getMembers(int shardId) {
        return members.get(shardId);
    }
	
	public Member getLocalMember() {
        return members.get(myShardId)[myProcessId];
    }
    
	public Member getMember(int shardId, String id) {
        for (Member member : members.get(shardId)) {
            if (member.getProcessId().equals(id)) {
                return member;
            }
        }
        throw new RococoException("Unable to locate a member by ID: " + id);
    }
	
	public Member getTpccShardMember(String table, String key) {
		int shardId, procId, index;
		switch (table) {
		// shard according to w_id and d_id
		case TPCCConstants.TABLENAME_DISTRICT:
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.lastIndexOf("_") + 1));
			index = (procId - 1) / TPCCScaleParameters.DIST_PER_NODE;
			return members.get(shardId - 1)[index];
		case TPCCConstants.TABLENAME_CUSTOMER: // w_d_?
		case TPCCConstants.TABLENAME_NEW_ORDER:
		case TPCCConstants.TABLENAME_ORDER:
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.indexOf("_") + 1, key.lastIndexOf("_")));
			index = (procId - 1) / TPCCScaleParameters.DIST_PER_NODE;
			return members.get(shardId - 1)[index];
		case TPCCConstants.TABLENAME_ORDER_LINE:
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.indexOf("_") + 1, key.indexOf("_", key.indexOf("_") + 1)));			
			index = (procId - 1) / TPCCScaleParameters.DIST_PER_NODE;
			return members.get(shardId - 1)[index];
		case TPCCConstants.TABLENAME_WAREHOUSE:
		case TPCCConstants.TABLENAME_ITEM:
		case TPCCConstants.TABLENAME_HISTORY:
			return members.get(0)[0];
		case TPCCConstants.TABLENAME_STOCK: // w_i
			shardId = Integer.parseInt(key.substring(0, key.indexOf("_")));
			procId = Integer.parseInt(key.substring(key.indexOf("_") + 1));
			index = (procId - 1) % TPCCScaleParameters.DIST_PER_NODE;
			return members.get(shardId - 1)[index];
		default:
			break;
		}
		return null;
	}
	
    public String getLogConfigFilePath(){
		return this.logConfigfile;
	}

	public int getShardId() {
		return myShardId;
	}

	public int getProcessId() {
		return myProcessId;
	}

}