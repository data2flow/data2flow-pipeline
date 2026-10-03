package net.java21.data2flow.pipeline.messaging;

import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.device.service.AutoRegisterQuota;
import net.java21.data2flow.pipeline.device.service.DeviceDirectory;
import net.java21.data2flow.pipeline.device.service.DeviceRuntimeCache;
import net.java21.data2flow.pipeline.device.service.SourceContextCache;
import net.java21.data2flow.pipeline.metric.service.MetricCatalogService;
import net.java21.data2flow.pipeline.script.service.ScriptRuntimeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

/**
 * 설정 변경 수신(EVT-ING-08, EVT-DEV-04·DSC-01·SCR-01, architecture.md §4.5). fanout {@code data2flow.config}를 인스턴스별 임시 큐로 받고,
 * 봉투는 공통 {@link ConfigChangedMessage}다. 내용이 아니라 {@code entityType}·{@code id}만 보고 해당 캐시를 지운 뒤 원천(core 내부 API)에서
 * 다시 읽는다. 연결이 다시 맺어지면(놓친 메시지가 있을 수 있음) 모든 캐시를 지운다.
 *
 * <table>
 *   <tr><th>entityType</th><th>동작</th></tr>
 *   <tr><td>DEVICE·ATTRIBUTE</td><td>기기 식별 캐시·기기 속성(id = 기기 ID). op=DELETE면 제거</td></tr>
 *   <tr><td>MODEL</td><td>그 모델의 기기 오프라인 기준이 바뀌므로 기기 정보를 다시 읽는다</td></tr>
 *   <tr><td>METRIC·ALIAS</td><td>조직 측정 항목·별칭 카탈로그</td></tr>
 *   <tr><td>SOURCE</td><td>소스 처리 정보(디코더·미등록 정책·한도)와 자동 등록 한도 창</td></tr>
 *   <tr><td>SCRIPT</td><td>조직 실행 번들 재적재(10초 안 반영, BR-SCR-09)와 적용 보고</td></tr>
 *   <tr><td>INGEST_CACHE</td><td>전체(조직 단위 구분 없이)</td></tr>
 * </table>
 */
public class ConfigChangeListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(ConfigChangeListener.class);

    private final MessageCodec codec = MessageCodec.create();
    private final DeviceDirectory devices;
    private final DeviceRuntimeCache runtimes;
    private final SourceContextCache sources;
    private final AutoRegisterQuota quota;
    private final MetricCatalogService catalogs;
    private final ScriptRuntimeRegistry scripts;

    public ConfigChangeListener(DeviceDirectory devices, DeviceRuntimeCache runtimes, SourceContextCache sources,
                                AutoRegisterQuota quota, MetricCatalogService catalogs, ScriptRuntimeRegistry scripts) {
        this.devices = devices;
        this.runtimes = runtimes;
        this.sources = sources;
        this.quota = quota;
        this.catalogs = catalogs;
        this.scripts = scripts;
    }

    @Override
    public void onMessage(Message message) {
        ConfigChangedMessage change;
        try {
            change = codec.read(message.getBody(), ConfigChangedMessage.class);
        } catch (RuntimeException e) {
            log.warn("읽을 수 없는 설정 변경 메시지를 무시합니다: {}", e.getMessage());
            return;
        }
        apply(change);
    }

    public void apply(ConfigChangedMessage change) {
        long id = parse(change.id());
        long org = parse(change.orgId());
        switch (change.entityType()) {
            case DEVICE, ATTRIBUTE -> {
                if (change.op() == ConfigChangedMessage.Op.DELETE && change.entityType() == ConfigChangedMessage.EntityType.DEVICE) {
                    devices.removeDevice(id);
                } else {
                    devices.invalidateDevice(id);
                }
                runtimes.invalidate(id);
            }
            case MODEL, SPACE, GROUP -> {
                devices.invalidateAll();
                runtimes.invalidateAll();
            }
            case METRIC, ALIAS -> catalogs.invalidate(org);
            case SOURCE, CREDENTIAL -> {
                sources.invalidate(id);
                quota.reset(id);
            }
            case SCRIPT -> {
                try {
                    scripts.reload(org);
                } catch (RuntimeException e) {
                    log.warn("스크립트 번들 재적재 실패(30초 폴링으로 따라잡음): {}", e.getMessage());
                }
            }
            case INGEST_CACHE -> invalidateAll();
            default -> {
                // 다른 서비스용(FLOW·SETTING 등)·모르는 종류는 무시
            }
        }
    }

    /** 재연결: 놓친 메시지가 있을 수 있으므로 원천에서 다시 읽게 한다 */
    public void invalidateAll() {
        devices.invalidateAll();
        runtimes.invalidateAll();
        sources.invalidateAll();
        catalogs.invalidateAll();
        try {
            scripts.invalidateAll();
        } catch (RuntimeException e) {
            log.warn("스크립트 번들 재적재 실패: {}", e.getMessage());
        }
    }

    private static long parse(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
