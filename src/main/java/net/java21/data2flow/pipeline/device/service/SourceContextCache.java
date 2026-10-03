package net.java21.data2flow.pipeline.device.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import net.java21.data2flow.pipeline.common.PipelineProperties;
import net.java21.data2flow.pipeline.device.domain.SourceContext;

import java.time.Duration;
import java.util.Optional;

/** 소스 처리 정보 캐시(API-ING-21). EVT-DSC-01 {@code SOURCE}·EVT-ING-08 {@code INGEST_CACHE}가 오면 지운다 */
public class SourceContextCache {

    private final CoreDirectory core;
    private final Cache<Long, Optional<SourceContext>> cache;

    public SourceContextCache(CoreDirectory core, PipelineProperties.Core settings) {
        this.core = core;
        this.cache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(settings.cacheTtl().multipliedBy(10).plus(Duration.ofSeconds(1)))
                .build();
    }

    public Optional<SourceContext> get(long sourceId) {
        return cache.get(sourceId, core::ingestContext);
    }

    public void invalidate(long sourceId) {
        cache.invalidate(sourceId);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }
}
