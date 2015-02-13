package cn.ict.rococo.txn;

public class TransactionFactory {


    public TransactionFactory() {
    	
    }
    
    public RococoTransaction create() {
    	return new RococoTransaction();
    }

    
}
