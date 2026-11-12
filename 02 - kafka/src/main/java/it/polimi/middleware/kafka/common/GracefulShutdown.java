package it.polimi.middleware.kafka.common;

import java.io.IOException;

public class GracefulShutdown {

    private final Thread shutdownThread;
    private final Runnable callback;
    private boolean requestedShutdown;

    public GracefulShutdown(Runnable callback) {
        shutdownThread = new Thread(this::readListener, "graceful-shutdown");
        shutdownThread.setDaemon(true);
        this.callback = callback;
        requestedShutdown = false;
    }

    private void readListener() {
        try {
            System.in.read();
        } catch (IOException ignored) {
        }
        requestedShutdown = true;
        if (callback != null) {
            callback.run();
        }
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
        return startDaemon(null);
    }

    public static GracefulShutdown startDaemon(Runnable callback) {
        final GracefulShutdown daemon = new GracefulShutdown(callback);
        daemon.start();
        return daemon;
    }

}
