package cn.ict.rcc.server.coordinator.messaging;

import java.util.concurrent.atomic.AtomicInteger;

public class TransactionFactory {
    
	public static AtomicInteger transactionIdGen = new AtomicInteger(0);

    public RococoTransaction create() {
    	return new RococoTransaction();
    }

}
