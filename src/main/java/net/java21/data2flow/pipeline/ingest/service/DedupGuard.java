package net.java21.data2flow.pipeline.ingest.service;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 중복 판정 ①의 파티션 캐시(BR-ING-07, reliability-and-ha.md §2.1): 파티션 하나는 활성 소비자 하나가 처리하므로 파티션별 최근 키
 * LRU(10분 창)로 DB 조회 없이 거른다. 활성 소비자가 바뀐 직후의 빈 캐시는 DB 조회(raw_messages)와 telemetry PK가 막는다.
 */
public class DedupGuard {

    private static final int MAX_PER_PARTITION = 50_000;

    private final Duration window;
    private final Map<Integer, LinkedHashMap<String, Instant>> partitions = new ConcurrentHashMap<>();

    public DedupGuard(Duration window) {
        this.window = window;
    }

    /** 창 안에 같은 키를 이 파티션에서 이미 처리했는지 */
    public boolean seen(int partition, long organizationId, String key, Instant receivedAt) {
        LinkedHashMap<String, Instant> cache = partitions.get(partition);
        if (cache == null) {
            return false;
        }
        synchronized (cache) {
            Instant at = cache.get(organizationId + "|" + key);
            return at != null && Duration.between(at, receivedAt).abs().compareTo(window) <= 0;
        }
    }

    public void record(int partition, long organizationId, String key, Instant receivedAt) {
        LinkedHashMap<String, Instant> cache = partitions.computeIfAbsent(partition, p -> new LinkedHashMap<>(1024, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Instant> eldest) {
                return size() > MAX_PER_PARTITION;
            }
        });
        synchronized (cache) {
            cache.put(organizationId + "|" + key, receivedAt);
        }
    }

    /** 활성 소비자가 바뀐 파티션 */
    public void clear(int partition) {
        partitions.remove(partition);
    }
}
