package it.polimi.nsds.akka.es4;

import java.io.IOException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;


public class Main {

    public static void main(String[] args) {

        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef server = sys.actorOf(ServerActor.props(), "server");
        final ActorRef client = sys.actorOf(ClientActor.props(server), "client");


        client.tell(new TextMessage("Message 1"),  ActorRef.noSender());
        client.tell(new StopMessage(), ActorRef.noSender());
        client.tell(new TextMessage("Message 2"),  ActorRef.noSender());
        client.tell(new WakeupMessage(), ActorRef.noSender());

        // Wait for all messages to be sent and received
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        sys.terminate();
    }
}
