package cn.ict.rcc.server.config;

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

import cn.ict.rcc.Member;
import cn.ict.rcc.benchmark.tpcc.TPCCConstants;
import cn.ict.rcc.benchmark.tpcc.TPCCScaleParameters;
import cn.ict.rcc.exception.RococoException;

public class ServerConfiguration {
	
	private int myShardId = 0;
	private int myProcessId = 0;
	private Map<Integer, Member[]> members = new HashMap<Integer, Member[]>();
	private String logConfigfile = "conf/log4j-server.properties";
	private String appServerUrl = "localhost:9190";
	
	private static final Log LOG = LogFactory.getLog(ServerConfiguration.class);
		
	private static volatile ServerConfiguration config = null;
	
	private ServerConfiguration(Properties properties) {
		logConfigfile = properties.getProperty("logConfigfile", "conf/log4j-server.properties");
		appServerUrl = properties.getProperty("rococo.coordinator");
		
		String shardIdValue = System.getProperty("rococo.shardId", properties.getProperty("rococo.shardId"));
		String processIdValue = System.getProperty("rococo.processId", properties.getProperty("rococo.processId"));
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
            members.put(entry.getKey(), entry.getValue().toArray(new Member[entry.getValue().size()]));
        }
	}
	
	public static ServerConfiguration getConfiguration() {
		if (config == null) {
			synchronized (ServerConfiguration.class) {
				if (config == null) {
					String configPath = System.getProperty("server.config.dir", "conf");
					Properties props = new Properties();
					File configFile = new File(configPath, "server.properties");
					try {
						props.load(new FileInputStream(configFile));
						config = new ServerConfiguration(props);
					} catch (IOException e) {
						String msg = "Error loading Micro configuration from: " + configFile.getPath();
						LOG.error(msg, e);
						throw new RococoException(msg, e);
					}
				}
			}
		}
		return config;
	}
	
	public Member getShardMember(String table, String key) {
		return Sharding_tpcc.getShardMember(members, table, key);
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
    
	public Member getMember(int shardId, String id) {
        for (Member member : members.get(shardId)) {
            if (member.getProcessId().equals(id)) {
                return member;
            }
        }
        throw new RococoException("Unable to locate a member by ID: " + id);
    }
}