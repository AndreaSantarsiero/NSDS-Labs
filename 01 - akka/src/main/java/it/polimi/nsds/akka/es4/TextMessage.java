package it.polimi.nsds.akka.es4;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Value;


@Value
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class TextMessage extends ClientMessage {

    String message;
}
