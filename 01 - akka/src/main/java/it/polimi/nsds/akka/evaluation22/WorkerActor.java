package it.polimi.nsds.akka.evaluation22;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import java.util.*;


public class WorkerActor extends AbstractActor {

    private final Map<String, List<ActorRef>> subscriptions = new HashMap<>();

    public WorkerActor() {}


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubscribeMsg.class, this::onSubscribeMsg)
                .match(PublishMsg.class, this::onPublishMsg).build();
    }


    void onSubscribeMsg(SubscribeMsg msg){
        if(subscriptions.get(msg.getTopic()) == null){
            ArrayList<ActorRef> list = new ArrayList<>();
            list.add(msg.getSender());
            subscriptions.put(msg.getTopic(), list);
        }
        else{
            subscriptions.get(msg.getTopic()).add(msg.getSender());
        }
    }

    void onPublishMsg(PublishMsg msg) throws Exception{
        List<ActorRef> subscribers = subscriptions.get(msg.getTopic());
        if(subscribers == null){
            throw new Exception("[WorkerActor] No subscribers found for this topic");
        }
        for(ActorRef subscriber : subscribers){
            NotifyMsg reply = new NotifyMsg(msg.getValue());
            subscriber.tell(reply,  ActorRef.noSender());
        }
    }


    static Props props() {
        return Props.create(WorkerActor.class);
    }
}
