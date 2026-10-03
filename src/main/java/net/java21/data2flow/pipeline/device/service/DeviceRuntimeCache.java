package net.java21.data2flow.pipeline.device.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.domain.DeviceRuntime;

/** 기기 실행 정보(API-DEV-122, 속성) 캐시. TRANSFORM이 있는 기기만 읽는다. DEVICE·ATTRIBUTE 변경 이벤트로 지운다 */
public class DeviceRuntimeCache {

    private final CoreDirectory core;
    private final Cache<Long, DeviceRuntime> cache;

    public DeviceRuntimeCache(CoreDirectory core, PipelineProperties.Core settings) {
        this.core = core;
        this.cache = Caffeine.newBuilder().maximumSize(100_000).expireAfterWrite(settings.cacheTtl()).build();
    }

    public DeviceRuntime get(long deviceId) {
        return cache.get(deviceId, core::deviceRuntime);
    }

    public void invalidate(long deviceId) {
        cache.invalidate(deviceId);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }
}
