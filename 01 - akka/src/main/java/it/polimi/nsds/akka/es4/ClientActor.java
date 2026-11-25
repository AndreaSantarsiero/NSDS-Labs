package it.polimi.nsds.akka.es4;

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
                .match(ClientMessage.class, this::onClientMessage)
                .match(ReplyMessage.class, this::onReplyMessage).build();
    }

    void onClientMessage(ClientMessage msg){
        System.out.println("[ClientActor] sending " + msg.getClass().getName());
        server.tell(msg, self());
    }

    void onReplyMessage(ReplyMessage msg){
        System.out.println("[ClientActor] receiving " + msg.getClass().getName());
    }

    static Props props(ActorRef server) {
        return Props.create(ClientActor.class,  () -> new ClientActor(server));
    }
}
