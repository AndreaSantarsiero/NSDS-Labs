package it.polimi.nsds.akka.evaluation22;

import akka.actor.*;
import akka.japi.pf.DeciderBuilder;
import java.time.Duration;


public class BrokerActor extends AbstractActorWithStash {

    private final ActorRef worker01;
    private final ActorRef worker02;

    public BrokerActor() {
        worker01 = getContext().actorOf(WorkerActor.props(), "worker01");
        worker02 = getContext().actorOf(WorkerActor.props(), "worker02");
    }


    private static SupervisorStrategy strategy =
            new OneForOneStrategy(
                    1,
                    Duration.ofMinutes(1),
                    DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.resume()).build());

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }


    @Override
    public Receive createReceive() {
        return batchOff();
    }

    public Receive batchOff(){
        return receiveBuilder()
                .match(BatchMsg.class, this::onBatchMsg)
                .match(SubscribeMsg.class, this::onSubscribeMsg)
                .match(PublishMsg.class, this::onPublishMsgWithBatchOff).build();
    }

    public Receive batchOn(){
        return receiveBuilder()
                .match(BatchMsg.class, this::onBatchMsg)
                .match(SubscribeMsg.class, this::onSubscribeMsg)
                .match(PublishMsg.class, this::onPublishMsgWithBatchOn).build();
    }


    void onBatchMsg(BatchMsg msg){
        if(msg.isOn()){
            getContext().become(batchOn());
        }
        else{
            getContext().become(batchOff());
            unstashAll();
        }
    }

    void onSubscribeMsg(SubscribeMsg msg){
        if(msg.getKey() % 2 == 0){
            System.out.println("[BrokerActor] Received SubscribeMsg with even key -> worker 01");
            worker01.tell(msg, self());
        }
        else {
            System.out.println("[BrokerActor] Received SubscribeMsg with odd key -> worker 02");
            worker02.tell(msg, self());
        }
    }

    void onPublishMsgWithBatchOff(PublishMsg msg) {
        worker01.tell(msg, getSelf());
        worker02.tell(msg, getSelf());
    }

    void onPublishMsgWithBatchOn(PublishMsg msg) {
        stash();
    }


    static Props props() {
        return Props.create(BrokerActor.class);
    }
}
