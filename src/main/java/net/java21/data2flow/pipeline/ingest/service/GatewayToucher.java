package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 게이트웨이 자동 등록·마지막 수신 갱신(DEV-05.01, BR-DEV-19): 업링크 rxInfo의 게이트웨이 EUI를 모아 1분마다
 * core API-DEV-125로 보낸다. 보내지 못하면 다음 주기에 다시 보낸다.
 */
public class GatewayToucher {

    private static final Logger log = LoggerFactory.getLogger(GatewayToucher.class);

    private final CoreDirectory core;
    private final Map<Key, Instant> pending = new ConcurrentHashMap<>();

    public GatewayToucher(CoreDirectory core) {
        this.core = core;
    }

    public void record(long organizationId, long sourceId, String gatewayEui, Instant seenAt) {
        pending.merge(new Key(organizationId, sourceId, gatewayEui), seenAt, (a, b) -> a.isAfter(b) ? a : b);
    }

    public int pendingCount() {
        return pending.size();
    }

    /** 모은 것을 보낸다(스케줄러 1분) */
    public void flush() {
        if (pending.isEmpty()) {
            return;
        }
        List<Map.Entry<Key, Instant>> batch = new ArrayList<>(pending.entrySet());
        List<CoreDirectory.GatewayTouch> items = batch.stream()
                .map(e -> new CoreDirectory.GatewayTouch(e.getKey().organizationId(), e.getKey().sourceId(),
                        e.getKey().gatewayEui(), e.getValue()))
                .toList();
        try {
            core.touchGateways(items);
            batch.forEach(e -> pending.remove(e.getKey(), e.getValue()));
        } catch (RuntimeException e) {
            log.warn("게이트웨이 수신 갱신 실패(다음 주기에 다시 보냄, {}건): {}", items.size(), e.getMessage());
        }
    }

    private record Key(long organizationId, long sourceId, String gatewayEui) {
    }
}
