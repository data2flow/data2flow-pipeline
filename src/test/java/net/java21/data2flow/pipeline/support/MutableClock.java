package net.java21.data2flow.pipeline.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** 테스트 시계(design/testing: Thread.sleep 대신 시간을 앞으로 돌린다) */
public final class MutableClock extends Clock {

    public static final Instant T0 = Instant.parse("2026-10-03T00:00:00Z");

    private volatile Instant now;

    public MutableClock(Instant now) {
        this.now = now;
    }

    public static MutableClock atUtc(String isoInstant) {
        return new MutableClock(Instant.parse(isoInstant));
    }

    public void set(Instant instant) {
        now = instant;
    }

    public void advance(Duration duration) {
        now = now.plus(duration);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return now;
    }
}
