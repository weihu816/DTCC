package cn.ict.rcc.server.coordinator.txn;

public class TransactionFactory {
    
    public RococoTransaction create() {
    	return new RococoTransaction();
    }

}
