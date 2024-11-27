package com.faultTolerance.counter;

import akka.actor.AbstractActor;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import java.time.Duration;

public class CounterSupervisorActor extends AbstractActor {

	 // #strategy
    private static SupervisorStrategy strategy =
        new OneForOneStrategy(
            1, // Max no of retries (max number of faults in the time span)
            Duration.ofMinutes(1), // Within what time period
            DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.restart())	// The strategy restarts the node. Set this to resume() or stop() to see different outputs
                .build());

    @Override
    public SupervisorStrategy supervisorStrategy() {
      return strategy;
    }

	public CounterSupervisorActor() {
	}

	@Override
	public Receive createReceive() {
		// Creates the child actor within the supervisor actor context
		return receiveBuilder()
		          .match(
		              Props.class,
		              props -> {
		                getSender().tell(getContext().actorOf(props), getSelf());
		              })
		          .build();
	}

	static Props props() {
		return Props.create(CounterSupervisorActor.class);
	}

}
