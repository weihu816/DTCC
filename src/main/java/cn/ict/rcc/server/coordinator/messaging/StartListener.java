package cn.ict.rcc.server.coordinator.messaging;

import cn.ict.rcc.messaging.ReturnType;

public interface StartListener {
	
	public void notifyOutcome(ReturnType returnType, int piece_number);
	
}
