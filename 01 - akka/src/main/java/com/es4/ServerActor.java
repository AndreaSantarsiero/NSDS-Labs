package com.es4;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;


public class ServerActor extends AbstractActorWithStash {

    public ServerActor() {}

    // Initial context setup
    @Override
    public Receive createReceive() {
        return awake();
    }

    public Receive awake() {
        return receiveBuilder()
                .match(TextMessage.class, this::onTextMessageWhileAwake)
                .match(StopMessage.class, this::onStopMessage)
                .match(WakeupMessage.class, this::onWakeupMessage).build();
    }

    public Receive stopped() {
        return receiveBuilder()
                .match(TextMessage.class, this::onTextMessageWhileStopped)
                .match(StopMessage.class, this::onStopMessage)
                .match(WakeupMessage.class, this::onWakeupMessage).build();
    }


    void onStopMessage(StopMessage msg){
        getContext().become(stopped());
    }

    void onWakeupMessage(WakeupMessage msg){
        getContext().become(awake());
        unstash();
    }

    void onTextMessageWhileAwake(TextMessage msg){
        System.out.println("[Server - awake] Received message while awake -> answering");
        ReplyMessage reply = new ReplyMessage(msg.getMessage());
        sender().tell(reply, self());
    }

    void onTextMessageWhileStopped(TextMessage msg){
        System.out.println("[Server - stopped] Received message while sleeping -> stashed");
        stash();
    }


    static Props props() {
        return Props.create(ServerActor.class);
    }
}
