package net.java21.data2flow.pipeline.device.service;

import net.java21.data2flow.pipeline.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** ING-07.02 TC-ING-083 · AT-ING-03.3 · BR-ING-09: 소스당 고정 1시간 창 한도, 첫 거부만 알람, 다음 창·해제 후 허용 */
class AutoRegisterQuotaTest {

    private final MutableClock clock = MutableClock.atUtc("2026-07-01T00:00:00Z");
    private final AutoRegisterQuota quota = new AutoRegisterQuota(clock);

    @Test
    @DisplayName("[ING-07.02][AT-ING-03.3] TC-ING-083 100대까지 허용, 101번째부터 거부(첫 거부에만 알람)")
    void hourlyLimit() {
        for (int i = 0; i < 100; i++) {
            assertThat(quota.tryAcquire(3, 100).allowed()).isTrue();
        }
        AutoRegisterQuota.Decision first = quota.tryAcquire(3, 100);
        AutoRegisterQuota.Decision second = quota.tryAcquire(3, 100);

        assertThat(first.allowed()).isFalse();
        assertThat(first.firstRejection()).isTrue();
        assertThat(second.firstRejection()).isFalse();
        assertThat(quota.tryAcquire(4, 100).allowed()).as("다른 소스는 따로 센다").isTrue();
    }

    @Test
    @DisplayName("[ING-07.02][AT-ING-03.3] TC-ING-083 다음 시간 창에서 다시 허용, 한도 해제 후 즉시 허용, core 429면 창을 막는다")
    void windowAndReset() {
        quota.tryAcquire(3, 1);
        assertThat(quota.tryAcquire(3, 1).allowed()).isFalse();
        clock.advance(Duration.ofHours(1));
        assertThat(quota.tryAcquire(3, 1).allowed()).isTrue();
        assertThat(quota.markExceeded(3)).isTrue();
        assertThat(quota.markExceeded(3)).isFalse();
        assertThat(quota.tryAcquire(3, 100).allowed()).isFalse();
        quota.reset(3);
        assertThat(quota.tryAcquire(3, 1).allowed()).isTrue();
    }
}
