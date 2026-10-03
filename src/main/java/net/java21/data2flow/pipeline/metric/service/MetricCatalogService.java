package net.java21.data2flow.pipeline.metric.service;

import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import net.java21.data2flow.pipeline.device.service.CoreUnavailableException;
import net.java21.data2flow.pipeline.metric.domain.MetricCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 조직별 측정 항목·별칭 캐시(API-DEV-123, ING-04.03·DEV-04.03). EVT-DEV-04 {@code METRIC}·{@code ALIAS}가 오면 다음 사용 때 다시 읽고
 * ({@code sinceVersion}), 1분마다 새 버전이 있는지 본다. 처음 보는 키는 core에 UNVERIFIED로 등록을 요청한다(API-DEV-124,
 * ING-04.02). 같은 키를 동시에 여러 메시지가 보내도 등록 요청은 한 번이다(TC-ING-056).
 */
public class MetricCatalogService {

    private static final Logger log = LoggerFactory.getLogger(MetricCatalogService.class);

    private final CoreDirectory core;
    private final Map<Long, MetricCatalog> catalogs = new ConcurrentHashMap<>();
    private final Set<Long> stale = ConcurrentHashMap.newKeySet();
    /** 등록을 요청해서 UNVERIFIED가 된 키(카탈로그를 다시 읽을 때까지) */
    private final Map<Long, Set<String>> registered = new ConcurrentHashMap<>();

    public MetricCatalogService(CoreDirectory core) {
        this.core = core;
    }

    public MetricCatalog catalog(long organizationId) {
        MetricCatalog current = catalogs.get(organizationId);
        if (current != null && !stale.contains(organizationId)) {
            return current;
        }
        try {
            return refresh(organizationId);
        } catch (CoreUnavailableException e) {
            if (current != null) {
                log.warn("측정 항목 정의를 다시 읽지 못해 이전 값을 씁니다(org={}): {}", organizationId, e.getMessage());
                return current;
            }
            throw e;
        }
    }

    private MetricCatalog refresh(long organizationId) {
        MetricCatalog current = catalogs.get(organizationId);
        Long since = current == null ? null : current.version();
        MetricCatalog fresh = core.metrics(organizationId, since).orElse(current == null ? MetricCatalog.EMPTY : current);
        catalogs.put(organizationId, fresh);
        stale.remove(organizationId);
        if (current == null || fresh.version() != current.version()) {
            registered.remove(organizationId);
        }
        return fresh;
    }

    /** 1분 폴링 */
    public void refreshAll() {
        for (Long org : Set.copyOf(catalogs.keySet())) {
            try {
                refresh(org);
            } catch (RuntimeException e) {
                log.warn("측정 항목 정의 갱신 실패(org={}): {}", org, e.getMessage());
            }
        }
    }

    public void invalidate(long organizationId) {
        stale.add(organizationId);
    }

    public void invalidateAll() {
        stale.addAll(catalogs.keySet());
    }

    /** 이미 UNVERIFIED로 등록을 요청한 키인지 */
    public boolean isRegisteredUnverified(long organizationId, String key) {
        Set<String> keys = registered.get(organizationId);
        return keys != null && keys.contains(key);
    }

    /**
     * 처음 보는 키를 UNVERIFIED로 등록한다(이미 요청한 키는 건너뜀). core 장애면 CoreUnavailableException.
     *
     * @param samples 키 → 표본 값
     */
    public void registerUnverified(long organizationId, long deviceId, Map<String, Double> samples) {
        Set<String> keys = registered.computeIfAbsent(organizationId, k -> ConcurrentHashMap.newKeySet());
        Map<String, Double> todo = new LinkedHashMap<>();
        samples.forEach((k, v) -> {
            if (!keys.contains(k)) {
                todo.put(k, v);
            }
        });
        if (todo.isEmpty()) {
            return;
        }
        synchronized (keys) {
            List<CoreDirectory.UnverifiedKey> request = new ArrayList<>();
            todo.forEach((k, v) -> {
                if (!keys.contains(k)) {
                    request.add(new CoreDirectory.UnverifiedKey(k, deviceId, v));
                }
            });
            if (request.isEmpty()) {
                return;
            }
            core.registerUnverified(organizationId, request);
            request.forEach(r -> keys.add(r.key()));
        }
    }
}
