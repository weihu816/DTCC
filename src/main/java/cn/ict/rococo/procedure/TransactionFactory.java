package cn.ict.rococo.procedure;

public class TransactionFactory {


    public TransactionFactory() {
    	
    }
    
    public RococoTransaction create() {
    	return new RococoTransaction();
    }

    
}
