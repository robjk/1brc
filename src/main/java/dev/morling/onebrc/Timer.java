package dev.morling.onebrc;

import java.util.concurrent.TimeUnit;

/**
 * utility class for rough & ready timing
 */
public class Timer {
    private long start;
    private long end;

    public Timer() {
        start();
    }

    public void start() {
        start = System.nanoTime();
    }

    public void end() {
        end = System.nanoTime();
    }

    @Override
    public String toString() {
        end();
        return "%d ms".formatted(TimeUnit.NANOSECONDS.toMillis(end - start));
    }
}
