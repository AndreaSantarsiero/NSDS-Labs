package it.polimi.nsds.akka.es2;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;


public class CounterActor extends AbstractActorWithStash {

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
        unstash();
    }

    void onCounterDecrease(CounterDecrease msg){
        if(counter <= 0){
            stash();
            return;
        }
        this.counter--;
        System.out.println("Counter decreased to " + counter);
    }

    static Props props() {
        return Props.create(CounterActor.class);
    }

}
