package it.polimi.nsds.akka.es3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;


public class Main {

    private static final int numThreads = 10;


    public static void main(String[] args) {

        final ActorSystem sys = ActorSystem.create("System");
        final ActorRef server = sys.actorOf(ServerActor.props(), "server");
        final ActorRef client = sys.actorOf(ClientActor.props(server), "client");


        final Map<String, String> addressBook  = new HashMap<>();
        addressBook.put("Mario Rossi", "mariorossi@gmail.com");
        addressBook.put("Pietro Bianchi", "pietrobianchi@gmail.com");
        addressBook.put("Luca Verdi", "lucaverdi@gmail.com");
        addressBook.put("Chiara Gialli", "chiaragialli@gmail.com");
        addressBook.put("Martina Neri", "martinaneri@gmail.com");


        // Send messages from multiple threads in parallel
        final ExecutorService exec = Executors.newFixedThreadPool(numThreads);

        for (Map.Entry<String, String> user : addressBook.entrySet()) {
            exec.submit(() -> client.tell(new GetMessageRequest(user.getKey()), ActorRef.noSender()));
            exec.submit(() -> client.tell(new PutMessageRequest(user.getKey(), user.getValue()), ActorRef.noSender()));
        }

        // Wait for all messages to be sent and received
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        exec.shutdown();
        sys.terminate();
    }
}
