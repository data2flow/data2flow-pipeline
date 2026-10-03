package net.java21.data2flow.pipeline.device.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 기기 식별 캐시(ING-03.01 TC-ING-046): (sourceId, externalId) → 기기. 미적중이면 core API-DEV-120을 <b>키마다 한 번만</b>
 * 부르고(동시 요청은 한 번의 호출을 기다림, single-flight), 없는 기기는 30초 동안 음성 캐시한다. 기기가 바뀌면(EVT-DEV-04
 * {@code DEVICE}) 즉시 지운다. 오프라인 판정용으로 기기 ID → 정보도 들고 있고, 시작할 때와 5분마다 API-DEV-130으로 채운다.
 */
public class DeviceDirectory {

    private static final Logger log = LoggerFactory.getLogger(DeviceDirectory.class);

    private final CoreDirectory core;
    private final Cache<Key, Optional<DeviceInfo>> byExternal;
    private final Map<Long, DeviceInfo> byId = new ConcurrentHashMap<>();
    private volatile Instant warmedUntil;

    public DeviceDirectory(CoreDirectory core, PipelineProperties.Core settings) {
        this.core = core;
        long positive = Math.max(settings.cacheTtl().toNanos(), Duration.ofSeconds(1).toNanos()) * 10;
        long negative = settings.negativeCacheTtl().toNanos();
        this.byExternal = Caffeine.newBuilder()
                .maximumSize(200_000)
                .expireAfter(Expiry.<Key, Optional<DeviceInfo>>writing((k, v) -> Duration.ofNanos(v.isPresent() ? positive : negative)))
                .build();
    }

    /** 외부 ID 정규화(소문자, DEV domain-model {@code devices.external_id}) */
    public static String normalize(String externalId) {
        return externalId == null ? null : externalId.trim().toLowerCase(Locale.ROOT);
    }

    /** 기기 조회. core 장애면 {@link CoreUnavailableException} */
    public Optional<DeviceInfo> find(long sourceId, String externalId) {
        Key key = new Key(sourceId, normalize(externalId));
        Optional<DeviceInfo> found = byExternal.get(key, k -> core.findDevice(k.sourceId(), k.externalId())
                .filter(d -> !d.deleted()));
        found.ifPresent(d -> byId.put(d.deviceId(), d));
        return found;
    }

    /** 자동 등록 등으로 알게 된 기기를 넣는다 */
    public void put(DeviceInfo device) {
        byExternal.put(new Key(device.sourceId(), normalize(device.externalId())), Optional.of(device));
        byId.put(device.deviceId(), device);
    }

    public Optional<DeviceInfo> byId(long deviceId) {
        return Optional.ofNullable(byId.get(deviceId));
    }

    public Collection<DeviceInfo> known() {
        return byId.values();
    }

    /** EVT-DEV-04 {@code DEVICE}·{@code MODEL}: 그 기기 캐시를 지운다. 모르는 기기면 음성 캐시를 모두 지운다(새로 만든 기기일 수 있음) */
    public void invalidateDevice(long deviceId) {
        DeviceInfo known = byId.get(deviceId);
        if (known != null) {
            byExternal.invalidate(new Key(known.sourceId(), normalize(known.externalId())));
        }
        byExternal.asMap().entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    /** 기기 삭제(op=DELETE) */
    public void removeDevice(long deviceId) {
        invalidateDevice(deviceId);
        byId.remove(deviceId);
    }

    /** 재연결·INGEST_CACHE: 전체를 다시 읽게 한다(원천은 DB) */
    public void invalidateAll() {
        byExternal.invalidateAll();
        warmedUntil = null;
    }

    /** 오프라인 판정용 기기 정보 채우기(API-DEV-130, 바뀐 것만) */
    public void warm(Instant now) {
        Instant since = warmedUntil;
        int page = 1;
        try {
            while (true) {
                CoreDirectory.DevicePage result = core.listDevices(since, page, 100);
                for (DeviceInfo d : result.devices()) {
                    if (d.deleted()) {
                        removeDevice(d.deviceId());
                    } else {
                        byId.put(d.deviceId(), d);
                        byExternal.put(new Key(d.sourceId(), normalize(d.externalId())), Optional.of(d));
                    }
                }
                if (result.devices().isEmpty() || page >= result.totalPages()) {
                    break;
                }
                page++;
            }
            warmedUntil = now.minus(Duration.ofMinutes(1));
        } catch (RuntimeException e) {
            log.warn("기기 목록을 읽지 못했습니다(다음 주기에 다시 시도): {}", e.getMessage());
        }
    }

    private record Key(long sourceId, String externalId) {
    }
}
