package net.java21.data2flow.pipeline.device.service;

import net.java21.data2flow.pipeline.device.domain.AutoRegisterResult;
import net.java21.data2flow.pipeline.device.domain.DeviceInfo;
import net.java21.data2flow.pipeline.device.domain.DeviceRuntime;
import net.java21.data2flow.pipeline.device.domain.SourceContext;
import net.java21.data2flow.pipeline.metric.domain.MetricCatalog;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** 단위 테스트용 core 대역(메모리) */
public class FakeCoreDirectory implements CoreDirectory {

    public final Map<String, DeviceInfo> devices = new ConcurrentHashMap<>();
    public final AtomicInteger findCalls = new AtomicInteger();
    public final AtomicInteger metricCalls = new AtomicInteger();
    public final List<List<UnverifiedKey>> unverified = new CopyOnWriteArrayList<>();
    public volatile MetricCatalog catalog = MetricCatalog.EMPTY;
    public volatile RuntimeBundle bundle = RuntimeBundle.EMPTY;
    public final List<String> acks = new CopyOnWriteArrayList<>();
    public volatile boolean down;

    @Override
    public Optional<SourceContext> ingestContext(long sourceId) {
        return Optional.empty();
    }

    @Override
    public Optional<DeviceInfo> findDevice(long sourceId, String externalId) {
        findCalls.incrementAndGet();
        if (down) {
            throw new CoreUnavailableException("down", null);
        }
        return Optional.ofNullable(devices.get(sourceId + "|" + externalId));
    }

    @Override
    public AutoRegisterResult autoRegister(AutoRegisterCommand command) {
        return new AutoRegisterResult(1L, true, AutoRegisterResult.Outcome.REGISTERED);
    }

    @Override
    public DeviceRuntime deviceRuntime(long deviceId) {
        return new DeviceRuntime(deviceId, null);
    }

    @Override
    public DevicePage listDevices(Instant updatedAfter, int page, int size) {
        if (down) {
            throw new CoreUnavailableException("down", null);
        }
        return new DevicePage(List.copyOf(devices.values()), 1, 1);
    }

    @Override
    public Optional<MetricCatalog> metrics(long organizationId, Long sinceVersion) {
        metricCalls.incrementAndGet();
        if (down) {
            throw new CoreUnavailableException("down", null);
        }
        if (sinceVersion != null && sinceVersion == catalog.version()) {
            return Optional.empty();
        }
        return Optional.of(catalog);
    }

    @Override
    public List<String> registerUnverified(long organizationId, List<UnverifiedKey> keys) {
        unverified.add(keys);
        return keys.stream().map(UnverifiedKey::key).toList();
    }

    @Override
    public void touchGateways(List<GatewayTouch> items) {
        if (down) {
            throw new CoreUnavailableException("down", null);
        }
    }

    @Override
    public RuntimeBundle runtimeBundle(long organizationId) {
        if (down) {
            throw new CoreUnavailableException("down", null);
        }
        return bundle;
    }

    @Override
    public void deployAck(String instance, long scriptId, long versionId, Instant appliedAt) {
        acks.add(instance + ":" + scriptId + ":" + versionId);
    }

    public static DeviceInfo device(long id, long sourceId, String externalId, Integer interval, Double multiplier,
                                    Integer modelInterval, Double modelMultiplier) {
        return new DeviceInfo(id, 1, sourceId, externalId, "d" + id, "ACTIVE", 1L, null, null, false, false, interval,
                multiplier, modelInterval, modelMultiplier, null);
    }
}
