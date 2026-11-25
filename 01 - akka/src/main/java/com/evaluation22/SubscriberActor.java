package com.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;


public class SubscriberActor extends AbstractActor {

    private ActorRef broker;

    public SubscriberActor() {}


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(NotifyMsg.class, this::onNotifyMsg)
                .match(SubscribeMsg.class, this::onSubscribeMsg)
                .match(SetBrokerMsg.class, this::onSetBrokerMsg).build();
    }


    void onNotifyMsg(NotifyMsg msg){
        System.out.println("[SubscriberActor] Received NotifyMsg with value " + msg.getValue());
    }

    void onSubscribeMsg(SubscribeMsg msg){
        broker.tell(msg, self());
    }

    void onSetBrokerMsg(SetBrokerMsg msg){
        broker = msg.getBroker();
    }


    static Props props() {
        return Props.create(SubscriberActor.class);
    }
}
