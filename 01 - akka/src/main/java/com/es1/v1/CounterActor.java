package com.es1.v1;

import akka.actor.AbstractActor;
import akka.actor.Props;


public class CounterActor extends AbstractActor {

    private int counter;

    public CounterActor() {
        this.counter = 0;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(CounterIncrease.class, this::onCounterIncrease)
                               .match(CounterDecrease.class, this::onCounterDecrease).build();
    }


    void onCounterIncrease(CounterIncrease msg){
        this.counter++;
        System.out.println("Counter increased to " + counter);
    }

    void onCounterDecrease(CounterDecrease msg){
        this.counter--;
        System.out.println("Counter decreased to " + counter);
    }

    static Props props() {
        return Props.create(CounterActor.class);
    }

}
