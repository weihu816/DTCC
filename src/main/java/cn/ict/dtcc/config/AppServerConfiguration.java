package cn.ict.dtcc.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.dtcc.exception.DTCCException;
import cn.ict.dtcc.util.DTCCUtil;

public class AppServerConfiguration {
	
	private static final String SHARDING_TPCC = "tpcc";
	private static final String SHARDING_MICRO = "micro";
	private int myShardId = 0;
	private int myProcessId = 0;
	private Map<Integer, Member[]> members = new HashMap<Integer, Member[]>();
	private Map<String, Member> membersIndex = new HashMap<String, Member>();
	private String logConfigfile = "conf/log4j-server.properties";
	private String appServerUrl = "localhost:9190";
	private String sharding = SHARDING_TPCC;
	
	private static final Log LOG = LogFactory.getLog(AppServerConfiguration.class);
		
	private static volatile AppServerConfiguration config = null;
	
	private AppServerConfiguration(Properties properties) {
		logConfigfile = properties.getProperty("logConfigfile", "conf/log4j-server.properties");
		appServerUrl = properties.getProperty("dtcc.app.server", "localhost:9190");
		
		String shardIdValue = System.getProperty("dtcc.shardId", properties.getProperty("dtcc.shardId"));
		String processIdValue = System.getProperty("dtcc.processId", properties.getProperty("dtcc.processId"));
		sharding = System.getProperty("dtcc.sharding", properties.getProperty("dtcc.sharding"));
		if (shardIdValue != null && processIdValue != null) {
			myShardId = Integer.parseInt(shardIdValue);
			myProcessId = Integer.parseInt(processIdValue);
		}

		Map<Integer,List<Member>> tempMembers = new HashMap<Integer,List<Member>>();
        for (String property : properties.stringPropertyNames()) {
            if (property.startsWith("dtcc.server")) {
                int shardId = Integer.parseInt(property.substring(12, property.indexOf('.', 13)));
                String value = properties.getProperty(property);
                String processId = property.substring(property.lastIndexOf('.') + 1);
                String[] connection = value.split(":");
                Member member = new Member(connection[0], Integer.parseInt(connection[1]), processId, false,
                		DTCCUtil.buildKey(shardId, processId));
                List<Member> temp = tempMembers.get(shardId);
                if (temp == null) {
                    temp = Collections.synchronizedList(new ArrayList<Member>());
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
            for (Member m : entry.getValue()) {
            	membersIndex.put(m.getId(), m);
            }
            members.put(entry.getKey(), entry.getValue().toArray(new Member[entry.getValue().size()]));
        }
	}
	
	public static AppServerConfiguration getConfiguration() {
		if (config == null) {
			synchronized (AppServerConfiguration.class) {
				if (config == null) {
					String configPath = System.getProperty("server.config.dir", "conf");
					Properties props = new Properties();
					File configFile = new File(configPath, "app-server.properties");
					try {
						props.load(new FileInputStream(configFile));
						config = new AppServerConfiguration(props);
					} catch (IOException e) {
						String msg = "Error loading Micro configuration from: " + configFile.getPath();
						LOG.error(msg, e);
						throw new DTCCException(msg, e);
					}
				}
			}
		}
		return config;
	}
	
	public Member getShardMember(String table, String key) {
		switch (sharding) {
		case SHARDING_TPCC:
			return Sharding_tpcc.getShardMember(members, table, key);
		case SHARDING_MICRO:
			return Sharding_micro.getShardMember(members, table, key);
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

	public String getAppServerUrl() {
        return appServerUrl;
    }
	
	public List<Member> getMembers() {
		List<Member> allMembers = new ArrayList<Member>();
		for (Entry<Integer, Member[]> e : members.entrySet()) {
			for (Member m : e.getValue()) {
				allMembers.add(m);
			}	
		}
        return allMembers;
    }
	
	public Member[] getMembers(int shardId) {
        return members.get(shardId);
    }
	
	public Member getLocalMember() {
        return members.get(myShardId)[myProcessId];
    }
    
	public Member getMember(int shardId, int id) {
		return members.get(shardId)[id];
    }
	
	public Member getMember(String id) {
		return membersIndex.get(id);
	}
}