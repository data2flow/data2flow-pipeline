package net.java21.data2flow.pipeline.metric.service;

import net.java21.data2flow.contracts.message.decoder.DecodedValue;
import net.java21.data2flow.pipeline.device.service.CoreUnavailableException;
import net.java21.data2flow.pipeline.device.service.FakeCoreDirectory;
import net.java21.data2flow.pipeline.metric.domain.MetricCatalog;
import net.java21.data2flow.pipeline.metric.domain.MetricDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ING-02.06 TC-ING-044 · ING-04.03 TC-ING-057 · ING-04.02: 값 변환, 별칭, UNVERIFIED 등록 1회 */
class MetricServicesTest {

    private static final MetricDefinition DOOR = new MetricDefinition(1, "door", null, "ENUM", null, null, "VERIFIED",
            Map.of("open", 1.0, "close", 0.0), true);
    private static final MetricDefinition TEMP = new MetricDefinition(2, "temperature", "℃", "NUMBER", -20.0, 60.0,
            "VERIFIED", Map.of(), false);

    @Test
    @DisplayName("[ING-02.06][AT-ING-02.5] TC-ING-044 open/close → 1/0, 불린 → 1/0, ON(대소문자 무시) → 1, 매핑에 없는 half → 실패")
    void coercion() {
        assertThat(MetricValueMapper.toNumber(new DecodedValue("magnet_status", "open", null), DOOR)).hasValue(1.0);
        assertThat(MetricValueMapper.toNumber(new DecodedValue("magnet_status", "close", null), DOOR)).hasValue(0.0);
        assertThat(MetricValueMapper.toNumber(new DecodedValue("x", true, null), null)).hasValue(1.0);
        assertThat(MetricValueMapper.toNumber(new DecodedValue("x", false, null), TEMP)).hasValue(0.0);
        assertThat(MetricValueMapper.toNumber(new DecodedValue("relay", "ON", null), null)).hasValue(1.0);
        assertThat(MetricValueMapper.toNumber(new DecodedValue("magnet_status", "half", null), DOOR)).isEmpty();
        assertThat(MetricValueMapper.toNumber(new DecodedValue("x", "22.5", null), null)).hasValue(22.5);
        assertThat(MetricValueMapper.toNumber(new DecodedValue("x", "abc", null), null)).isEmpty();
        assertThat(MetricValueMapper.toNumber(new DecodedValue("x", Double.NaN, null), null)).isEmpty();
    }

    @Test
    @DisplayName("[ING-04.03][AT-ING-02.5] TC-ING-057 illuminance → illumination, magnet_status → door, 별칭이 아니면 그대로")
    void aliases() {
        MetricCatalog catalog = new MetricCatalog(3, Map.of("door", DOOR, "temperature", TEMP),
                Map.of("illuminance", "illumination", "magnet_status", "door"));

        assertThat(catalog.canonicalKey("illuminance")).isEqualTo("illumination");
        assertThat(catalog.canonicalKey("magnet_status")).isEqualTo("door");
        assertThat(catalog.canonicalKey("temperature")).isEqualTo("temperature");
        assertThat(catalog.definition("door").stateType()).isTrue();
        assertThat(TEMP.outOfRange(85)).isTrue();
        assertThat(TEMP.outOfRange(60)).isFalse();
    }

    @Test
    @DisplayName("[ING-04.03][AT-ING-02.5] TC-ING-057 별칭 변경 이벤트 후 카탈로그를 다시 읽고(sinceVersion), core 장애면 이전 카탈로그를 쓴다")
    void catalogRefresh() {
        FakeCoreDirectory core = new FakeCoreDirectory();
        core.catalog = new MetricCatalog(1, Map.of(), Map.of());
        MetricCatalogService service = new MetricCatalogService(core);

        assertThat(service.catalog(1).version()).isEqualTo(1);
        assertThat(service.catalog(1).version()).isEqualTo(1);
        assertThat(core.metricCalls.get()).isEqualTo(1);
        core.catalog = new MetricCatalog(2, Map.of(), Map.of("illuminance", "illumination"));
        service.invalidate(1);
        assertThat(service.catalog(1).canonicalKey("illuminance")).isEqualTo("illumination");
        service.invalidateAll();
        core.down = true;
        assertThat(service.catalog(1).version()).isEqualTo(2);
        service.refreshAll();
        assertThatThrownBy(() -> service.catalog(9)).isInstanceOf(CoreUnavailableException.class);
    }

    @Test
    @DisplayName("[ING-04.02][AT-ING-04.2] TC-ING-056 처음 보는 키를 동시에 20번 보내도 등록 요청은 1번")
    void unverifiedOnce() throws Exception {
        FakeCoreDirectory core = new FakeCoreDirectory();
        MetricCatalogService service = new MetricCatalogService(core);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        for (int i = 0; i < 20; i++) {
            pool.submit(() -> service.registerUnverified(1, 17, Map.of("pm4_0", 12.0)));
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(core.unverified).hasSize(1);
        assertThat(core.unverified.getFirst()).extracting(k -> k.key()).isEqualTo(List.of("pm4_0"));
        assertThat(service.isRegisteredUnverified(1, "pm4_0")).isTrue();
        assertThat(service.isRegisteredUnverified(2, "pm4_0")).isFalse();
    }
}
