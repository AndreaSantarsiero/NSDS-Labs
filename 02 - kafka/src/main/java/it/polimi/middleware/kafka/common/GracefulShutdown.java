package it.polimi.middleware.kafka.common;

import java.io.IOException;

public class GracefulShutdown {

    private final Thread shutdownThread;
    private boolean requestedShutdown;

    public GracefulShutdown() {
        shutdownThread = new Thread(this::readListener, "graceful-shutdown");
        shutdownThread.setDaemon(true);
        requestedShutdown = false;
    }

    private void readListener() {
        try {
            System.in.read();
        } catch (IOException ignored) {
        }
        requestedShutdown = true;
    }

    public void start() {
        shutdownThread.start();
    }

    public void stop() {
        shutdownThread.interrupt();
    }

    public boolean requestedShutdown() {
        return requestedShutdown;
    }

    public static GracefulShutdown startDaemon() {
        final GracefulShutdown daemon = new GracefulShutdown();
        daemon.start();
        return daemon;
    }

}
