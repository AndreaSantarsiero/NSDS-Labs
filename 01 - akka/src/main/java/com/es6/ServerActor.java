package com.es6;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;
import java.time.Duration;


public class ServerActor extends AbstractActorWithStash {

    public ServerActor() {}

    // Initial context setup
    @Override
    public Receive createReceive() {
        return ready();
    }

    public Receive ready() {
        return receiveBuilder()
                .match(RequestMessage.class, this::onRequestMessageWhileReady).build();
    }

    public Receive processing() {
        return receiveBuilder()
                .match(RequestMessage.class, this::onRequestMessageWhileProcessing)
                .match(ProcessingMessage.class, this::onProcessingMessage).build();
    }


    void onRequestMessageWhileReady(RequestMessage msg){
        System.out.println("[ServerActor] Received RequestMessage while ready: processing...");
        ProcessingMessage processingMessage = new ProcessingMessage(msg.getMessage(), sender());
        getContext().system().scheduler().scheduleOnce(Duration.ofSeconds(3), self(), processingMessage, getContext().dispatcher(), self());
        getContext().become(processing());
    }

    void onRequestMessageWhileProcessing(RequestMessage msg){
        stash();
    }

    void onProcessingMessage(ProcessingMessage msg){
        ReplyMessage reply = new ReplyMessage(msg.getMessage().toUpperCase());
        System.out.println("[ServerActor] Processing done!");
        msg.getClient().tell(reply, self());
        getContext().become(ready());
        unstash();
    }


    static Props props() {
        return Props.create(ServerActor.class);
    }
}
