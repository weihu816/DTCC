package cn.ict.rococo.txn;

import org.apache.log4j.PropertyConfigurator;


public class test00 {
	
    public static void main(String[] args) {
    	
    	PropertyConfigurator.configure(CoordinatorClientConfiguration.getConfiguration()
				.getLogConfigFilePath());
    	
    	CoordinatorClient client = new CoordinatorClient();
    	client.NewOrder(1, 1);
        
    }

}
