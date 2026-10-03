package net.java21.data2flow.pipeline.connectivity.service;

import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.messaging.DomainEventPublisher;
import net.java21.data2flow.pipeline.telemetry.repository.DeviceStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 오프라인 판정(DEV-02.05, BR-DEV-08): 1분마다 {@code now - last_seen_at > 주기 × 배수}인 기기를 OFFLINE으로 바꾸고
 * {@code device.connectivity.changed}(EVT-DEV-02)를 낸다. 주기·배수는 기기 → 모델 → 시스템 기본값(300초, 3) 순서로 상속한다(DEV-02.08).
 * 기기 정보를 아직 모르는 기기(시작 직후 목록을 못 읽음)는 잘못된 판정을 막으려고 다음 주기로 미룬다.
 * 상태 변경과 이벤트 발행은 한 트랜잭션: 발행이 실패하면 상태를 되돌리고 다음 주기에 다시 판정한다.
 */
public class OfflineDetector {

    private static final Logger log = LoggerFactory.getLogger(OfflineDetector.class);

    private final DeviceStateRepository states;
    private final DeviceDirectory devices;
    private final DomainEventPublisher events;
    private final TransactionTemplate tx;
    private final PipelineProperties.Offline settings;
    private final Clock clock;

    public OfflineDetector(DeviceStateRepository states, DeviceDirectory devices, DomainEventPublisher events,
                           TransactionTemplate tx, PipelineProperties.Offline settings, Clock clock) {
        this.states = states;
        this.devices = devices;
        this.events = events;
        this.tx = tx;
        this.settings = settings;
        this.clock = clock;
    }

    /** 판정 한 번. OFFLINE으로 바꾼 기기 ID */
    public List<Long> detect() {
        Instant now = clock.instant();
        // 가장 짧은 기준(주기 10초 × 배수 1.5)보다 오래 조용한 기기만 후보
        List<DeviceStateRepository.DeviceState> candidates = states.findSeenBefore(now.minus(Duration.ofSeconds(15)));
        List<Long> offline = new ArrayList<>();
        for (DeviceStateRepository.DeviceState state : candidates) {
            Optional<DeviceInfo> info = devices.byId(state.deviceId());
            if (info.isEmpty()) {
                log.debug("기기 {} 정보를 몰라 오프라인 판정을 미룹니다", state.deviceId());
                continue;
            }
            DeviceInfo device = info.get();
            Duration threshold = device.offlineAfter(settings.defaultIntervalSec(), settings.defaultMultiplier());
            if (Duration.between(state.lastSeenAt(), now).compareTo(threshold) <= 0) {
                continue;
            }
            try {
                Boolean changed = tx.execute(s -> {
                    if (!states.updateOffline(state.organizationId(), state.deviceId(), state.lastSeenAt(), now)) {
                        return false;
                    }
                    DeviceConnectivityChanged.Connectivity from = "ONLINE".equals(state.connectivity())
                            ? DeviceConnectivityChanged.Connectivity.ONLINE : null;
                    events.publish(EventType.DEVICE_CONNECTIVITY_CHANGED, state.organizationId(),
                            new DeviceConnectivityChanged(state.deviceId(), from,
                                    DeviceConnectivityChanged.Connectivity.OFFLINE, state.lastSeenAt(),
                                    device.effectiveIntervalSec(settings.defaultIntervalSec()),
                                    device.effectiveMultiplier(settings.defaultMultiplier())));
                    return true;
                });
                if (Boolean.TRUE.equals(changed)) {
                    offline.add(state.deviceId());
                }
            } catch (RuntimeException e) {
                log.warn("기기 {} 오프라인 판정 실패(다음 주기에 다시): {}", state.deviceId(), e.getMessage());
            }
        }
        return offline;
    }
}
