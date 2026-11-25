package it.polimi.nsds.akka.es3;

import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class PutMessageRequest {
    
    private final String name;
    private final String email;
}
