package net.java21.data2flow.pipeline.device.service;

import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ING-03.01 TC-ING-046 · AT-ING-03.4: (sourceId, externalId) 캐시, 미적중 시 core 1회(동시 50개 single-flight), 음성 캐시 */
class DeviceDirectoryTest {

    private final FakeCoreDirectory core = new FakeCoreDirectory();
    private final DeviceDirectory directory = new DeviceDirectory(core, new PipelineProperties.Core("http://x",
            Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(60), Duration.ofSeconds(30), Duration.ofSeconds(30)));

    @Test
    @DisplayName("[ING-03.01][AT-ING-03.4] TC-ING-046 동시 요청 50개에도 core 조회는 1회, 외부 ID는 소문자로 정규화")
    void singleFlight() throws Exception {
        core.devices.put("3|24e124743d012436", FakeCoreDirectory.device(17, 3, "24e124743d012436", 60, null, null, null));
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Long>> results = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return directory.find(3, "24E124743D012436").orElseThrow().deviceId();
            }));
        }
        start.countDown();
        for (Future<Long> f : results) {
            assertThat(f.get()).isEqualTo(17);
        }
        pool.shutdown();

        assertThat(core.findCalls.get()).isEqualTo(1);
        assertThat(directory.byId(17)).isPresent();
    }

    @Test
    @DisplayName("[ING-03.01][AT-ING-03.4] TC-ING-046 없는 기기는 음성 캐시(다시 묻지 않음), 무효화하면 다시 묻는다")
    void negativeCacheAndInvalidation() {
        assertThat(directory.find(3, "missing")).isEmpty();
        assertThat(directory.find(3, "missing")).isEmpty();
        assertThat(core.findCalls.get()).isEqualTo(1);

        core.devices.put("3|missing", FakeCoreDirectory.device(99, 3, "missing", null, null, null, null));
        directory.invalidateDevice(99);
        assertThat(directory.find(3, "missing")).isPresent();
        assertThat(core.findCalls.get()).isEqualTo(2);

        directory.removeDevice(99);
        assertThat(directory.byId(99)).isEmpty();
        directory.invalidateAll();
        directory.find(3, "missing");
        assertThat(core.findCalls.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("[ING-03.01] core 장애면 CoreUnavailableException(처리는 다시 시도), 목록 예열은 실패해도 조용히 넘어간다")
    void coreDown() {
        core.down = true;
        assertThatThrownBy(() -> directory.find(3, "x")).isInstanceOf(CoreUnavailableException.class);
        directory.warm(Instant.EPOCH);
        core.down = false;
        core.devices.put("3|w", FakeCoreDirectory.device(5, 3, "w", null, null, null, null));
        directory.warm(Instant.EPOCH);
        assertThat(directory.known()).extracting(DeviceInfo::deviceId).contains(5L);
        directory.put(FakeCoreDirectory.device(6, 3, "Six", null, null, null, null));
        assertThat(directory.find(3, "six")).isPresent();
    }
}
