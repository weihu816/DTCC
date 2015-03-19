package cn.ict.rcc.server.coordinator.txn;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.rcc.exception.RococoException;

public class CoordinatorClientConfiguration {
	
	private static final Log log = LogFactory.getLog(CoordinatorClientConfiguration.class);
	
	private static volatile CoordinatorClientConfiguration config = null;
	
	private String appServerUrl = "rococo.app.server";
	private String logconfigfile = "conf/log4j-tools.properties";
	
	private CoordinatorClientConfiguration(Properties properties) {
		logconfigfile = properties.getProperty("logconfigfile", "conf/log4j-tools.properties");
		appServerUrl = properties.getProperty("rococo.app.server");
	}
	
	public static CoordinatorClientConfiguration getConfiguration() {
		
		if (config == null) {
            synchronized (CoordinatorClientConfiguration.class) {
                if (config == null) {
                    String configPath = System.getProperty("rococo.config.dir", "conf");
                    Properties props = new Properties();
                    File configFile = new File(configPath, "coordinatorClient.properties");
                    try {
                        props.load(new FileInputStream(configFile));
                        config = new CoordinatorClientConfiguration(props);
                    } catch (IOException e) {
                        String msg = "Error loading CoordinatorClient configuration from: " + configFile.getPath();
                        log.error(msg, e);
                        throw new RococoException(msg, e);
                    }
                }
            }
        }
        return config;
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

    public String getLogConfigFilePath(){
		return this.logconfigfile;
	}
}
