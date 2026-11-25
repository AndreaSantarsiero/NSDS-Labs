package com.es1.v2;

import akka.actor.AbstractActor;
import akka.actor.Props;


public class CounterActor extends AbstractActor {

    private int counter;

    public CounterActor() {
        this.counter = 0;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(DataMessage.class, this::onDataMessage).build();
    }

    void onDataMessage(DataMessage msg) {
        if(msg.getCode() == DataMessage.INCREASE) {
            counter++;
            System.out.println("Counter increased to " + counter);
        }
        else if(msg.getCode() == DataMessage.DECREASE) {
            counter--;
            System.out.println("Counter decreased to " + counter);
        }
        else {
            System.out.println("Unknown code " + msg.getCode());
        }
    }

    static Props props() {
        return Props.create(CounterActor.class);
    }

}
