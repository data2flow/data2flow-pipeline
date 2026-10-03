package net.java21.data2flow.pipeline.device.service;

import net.java21.data2flow.pipeline.device.domain.AutoRegisterResult;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.domain.DeviceRuntime;
import net.java21.data2flow.pipeline.device.domain.SourceContext;
import net.java21.data2flow.pipeline.metric.domain.MetricCatalog;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * core-api가 원천인 기준 정보(기기·소스·측정 항목·스크립트)를 pipeline이 읽는 창구. 운영 구현은 core 내부 API
 * ({@link CoreApiClient}), 원천은 DB이고 pipeline은 캐시만 한다(domain-map §6-6, ING domain-model 머리말).
 *
 * <p>core가 응답하지 않으면 {@link CoreUnavailableException}을 던진다. 처리 단계는 이것을 일시 장애로 보고 오프셋을 넘기지 않고
 * 다시 시도한다(유실 0).
 */
public interface CoreDirectory {

    /** API-ING-21. 없으면 빈 값 */
    Optional<SourceContext> ingestContext(long sourceId);

    /** API-DEV-120. 404 DEVICE_NOT_FOUND면 빈 값 */
    Optional<DeviceInfo> findDevice(long sourceId, String externalId);

    /** API-DEV-121 */
    AutoRegisterResult autoRegister(AutoRegisterCommand command);

    /** API-DEV-122 */
    DeviceRuntime deviceRuntime(long deviceId);

    /** API-DEV-130. updatedAfter 이후 바뀐 기기 한 페이지 */
    DevicePage listDevices(Instant updatedAfter, int page, int size);

    /** API-DEV-123. sinceVersion과 같으면(204) 빈 값 */
    Optional<MetricCatalog> metrics(long organizationId, Long sinceVersion);

    /** API-DEV-124. 등록된(또는 이미 있던) 키 */
    List<String> registerUnverified(long organizationId, List<UnverifiedKey> keys);

    /** API-DEV-125 */
    void touchGateways(List<GatewayTouch> items);

    /** API-SCR-32 */
    RuntimeBundle runtimeBundle(long organizationId);

    /** API-SCR-34 */
    void deployAck(String instance, long scriptId, long versionId, Instant appliedAt);

    /**
     * @param sourceMeta  원본 tags(location, point), deviceName 등(공간 자동 매핑 제안 ING-03.03)
     * @param metricKeys  첫 메시지의 측정 키
     */
    record AutoRegisterCommand(long organizationId, long sourceId, String externalId, String name, JsonNode sourceMeta,
                               Instant firstSeenAt, List<String> metricKeys) {
    }

    record DevicePage(List<DeviceInfo> devices, int page, int totalPages) {
    }

    record UnverifiedKey(String key, long deviceId, double sampleValue) {
    }

    record GatewayTouch(long organizationId, long sourceId, String gatewayEui, Instant seenAt) {
    }
}
