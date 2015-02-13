package cn.ict.rococo.coordinator.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import cn.ict.rococo.exception.RococoException;

public class CoordinatorConfiguration {
	
	private static final Log log = LogFactory.getLog(CoordinatorConfiguration.class);
	
	private static volatile CoordinatorConfiguration config = null;
	
	private Map<Integer,Member[]> members = new HashMap<Integer, Member[]>();
	private String appServerUrl = "rococo.app.server";
	private String logconfigfile = "conf/log4j-server.properties";
	
	private CoordinatorConfiguration(Properties properties) {
				
		logconfigfile = properties.getProperty("logconfigfile", "conf/log4j-server.properties");
		appServerUrl = properties.getProperty("rococo.app.server");
		
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
	
	public static CoordinatorConfiguration getConfiguration() {
		if (config == null) {
            synchronized (CoordinatorConfiguration.class) {
                if (config == null) {
                    String configPath = System.getProperty("rococo.config.dir", "conf");
                    Properties props = new Properties();
                    File configFile = new File(configPath, "coordinator.properties");
                    try {
                        props.load(new FileInputStream(configFile));
                        config = new CoordinatorConfiguration(props);
                    } catch (IOException e) {
                        String msg = "Error loading Coordinator configuration from: " + configFile.getPath();
                        log.error(msg, e);
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

    public Member[] getMembers(String shardKey) {
    	return members.get(getShardId(shardKey));
    }

    public int getShardId(String shardKey) {
        return Math.abs(shardKey.hashCode() % members.size());
    }

    public int getShards() {
        return members.size();
    }

    public String getAppServerUrl() {
        return appServerUrl;
    }
    
    public String getHost() {
    	String appServerURL = getAppServerUrl();
        return appServerURL.substring(0, appServerURL.indexOf(':'));
    }
    
    public int getPort() {
    	String appServerURL = getAppServerUrl();
        return Integer.parseInt(appServerURL.substring(appServerURL.indexOf(':') + 1));
    }

//    public boolean reorderMembers(int shardId, String primary) {
//        try {
//            getMember(shardId, primary);
//        } catch (Exception e) {
//            return false;
//        }
//
//        Member[] temp = new Member[members.get(shardId).length];
//        int index = 1;
//        for (Member member : members.get(shardId)) {
//            if (member.getProcessId().equals(primary)) {
//                temp[0] = member;
//            } else {
//                temp[index] = member;
//                index++;
//            }
//        }
//        members.put(shardId, temp);
//        return true;
//    }

    public Member getMember(int shardId, String id) {
        for (Member member : members.get(shardId)) {
            if (member.getProcessId().equals(id)) {
                return member;
            }
        }
        throw new RococoException("Unable to locate a member by ID: " + id);
    }
    
    public String getLogConfigFilePath(){
		return this.logconfigfile;
	}
}
