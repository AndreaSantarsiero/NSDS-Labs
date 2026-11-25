package com.evaluation22;

import akka.actor.ActorRef;
import lombok.Value;


@Value
public class SetBrokerMsg {

    ActorRef broker;
}
