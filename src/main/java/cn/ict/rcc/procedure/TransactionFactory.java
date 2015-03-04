package cn.ict.rcc.procedure;

public class TransactionFactory {


    public TransactionFactory() {
    	
    }
    
    public RococoTransaction create() {
    	return new RococoTransaction();
    }

    
}
