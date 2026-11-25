package com.es3;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;


public class ClientActor extends AbstractActor {

    private final ActorRef server;

    public ClientActor(ActorRef server) {
        this.server = server;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GetMessageRequest.class, this::onGetMessageRequest)
                .match(PutMessageRequest.class, this::onPutMessageRequest)
                .match(GetMessageReply.class, this::onGetMessageReply).build();
    }


    void onGetMessageRequest(GetMessageRequest msg){
        System.out.println("[ClientActor] sending GetMessageRequest");
        server.tell(msg, self());
    }

    void onPutMessageRequest(PutMessageRequest msg){
        System.out.println("[ClientActor] sending PutMessageRequest");
        server.tell(msg, self());
    }

    void onGetMessageReply(GetMessageReply msg){
        if(msg.isFound()){
            System.out.println("[ClientActor] received a GetMessageReply! " + msg.getName() + " -> " + msg.getEmail());
        }
        else {
            System.out.println("[ClientActor] received a GetMessageReply! No matches for " + msg.getName());
        }
    }

    static Props props(ActorRef server) {
        return Props.create(ClientActor.class,  () -> new ClientActor(server));
    }
}
