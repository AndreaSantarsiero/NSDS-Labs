package it.polimi.nsds.akka.es6;

import akka.actor.ActorRef;
import lombok.AllArgsConstructor;
import lombok.Value;


@Value
@AllArgsConstructor
public class ProcessingMessage {

    String message;
    ActorRef client;
}
