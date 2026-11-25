package it.polimi.nsds.akka.es3;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class GetMessageRequest {

    private final String name;
}
