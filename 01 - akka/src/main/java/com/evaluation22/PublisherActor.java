package com.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;


public class PublisherActor extends AbstractActor {

    private ActorRef broker;

    public PublisherActor() {}


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PublishMsg.class, this::onPublishMsg)
                .match(SetBrokerMsg.class, this::onSetBrokerMsg).build();
    }


    void onPublishMsg(PublishMsg msg){
        broker.tell(msg, self());
    }

    void onSetBrokerMsg(SetBrokerMsg msg){
        broker = msg.getBroker();
    }


    static Props props() {
        return Props.create(PublisherActor.class);
    }
}
