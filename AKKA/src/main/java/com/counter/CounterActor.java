package com.counter;

import akka.actor.AbstractActor;
import akka.actor.Props;

/**
 * Notice: this class handles both SimpleMessage and OtherMessage, sent by the main method (entry point in Counter.java).
 * The interleaving of receipt for SimpleMessages and OtherMessages is completely random and non predictable in any way.
 */
public class CounterActor extends AbstractActor {

	private int counter;

	public CounterActor() {
		this.counter = 0;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
		.match(SimpleMessage.class, this::onMessage)
		.match(OtherMessage.class, this::onOtherMessage)
		.build();
	}

	void onMessage(SimpleMessage msg) {
		++counter;
		System.out.println("Counter increased to " + counter);
	}

	void onOtherMessage(OtherMessage msg) {
		System.out.println("Received an other type of message!");
	}

	static Props props() {
		return Props.create(CounterActor.class);
	}

}
