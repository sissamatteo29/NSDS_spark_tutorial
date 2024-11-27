package com.faultTolerance.counter;


/**
 * Message containing a code that will determine behaviour.
 * Behaviour upon receipt of a message can be parametrized:
 * - Based on the type of the message (SimpleMessage, OtherMessage, DataMessage...)
 * - Based on internal members of the message
 */
public class DataMessage {

	private int code;
	
	public int getCode() {
		return code;
	}

	public DataMessage(int code) {
		this.code = code;
	}
	
}
