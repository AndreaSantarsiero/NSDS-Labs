package it.polimi.nsds.akka.es3;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class GetMessageReply {

    private final String name;
    private final String email;
    private final boolean found;
}
