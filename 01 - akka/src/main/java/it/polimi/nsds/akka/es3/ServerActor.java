package it.polimi.nsds.akka.es3;

import akka.actor.AbstractActor;
import akka.actor.Props;
import java.util.HashMap;
import java.util.Map;


public class ServerActor extends AbstractActor {

    private final Map<String, String> addressBook;

    public ServerActor() {
        this.addressBook = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(GetMessageRequest.class, this::onGetMessage)
                               .match(PutMessageRequest.class, this::onPutMessage).build();
    }


    // Ask pattern: when a client request must be replied by exactly one reply message
    void onGetMessage(GetMessageRequest msg){
        String email = addressBook.getOrDefault(msg.getName(), null);
        if (email != null) {
            System.out.println("[ServerActor] onGetMessage: match! " + msg.getName() + "->" + email);
            GetMessageReply reply = new GetMessageReply(msg.getName(), email, true);
            sender().tell(reply,  self());
        }
        else {
            System.out.println("[ServerActor] onGetMessage: error! for name " + msg.getName());
            GetMessageReply reply = new GetMessageReply(msg.getName(), null, false);
            sender().tell(reply, self());
        }
    }

    void onPutMessage(PutMessageRequest msg){
        addressBook.put(msg.getName(), msg.getEmail());
        System.out.println("[ServerActor] onPutMessage: " + msg.getName() + " -> " + msg.getEmail());
    }

    static Props props() {
        return Props.create(ServerActor.class);
    }
}
