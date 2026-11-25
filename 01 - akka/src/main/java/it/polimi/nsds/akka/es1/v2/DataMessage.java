package it.polimi.nsds.akka.es1.v2;


public class DataMessage {

    public static final int DECREASE = 0;
    public static final int INCREASE = 1;

    private final int code;


    public DataMessage(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
