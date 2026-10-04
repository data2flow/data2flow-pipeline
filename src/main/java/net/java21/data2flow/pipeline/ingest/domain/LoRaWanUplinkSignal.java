package net.java21.data2flow.pipeline.ingest.domain;

import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.decoder.DecoderKeys;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * LoRaWAN 업링크 신호(EVT-ACT-07 {@code device.state.reported}, ACT-07.02, ADR-049 남은 것 ②). Class A 기기는 업링크 직후에만 다운링크를
 * 받으므로, action은 {@code QUEUED_FOR_DOWNLINK} 명령을 그 기기의 다음 업링크 직후 보낸다. pipeline은 ChirpStack 업링크
 * ({@code application/+/device/+/event/up}, 디코더 {@code chirpstack-v4})를 처리해 표준 메시지를 낼 때마다 이 신호를 함께 낸다.
 *
 * <ul>
 *   <li>대상: 실시간 처리 결과 OK(표준 메시지가 있음)인 <b>승인된(ACTIVE) 실제 기기</b>의 LoRaWAN 업링크만. 가상 기기(시뮬레이터가 직접 상태를
 *       보고), 승인 대기·비활성 기기(제어 불가), 다른 소스(MQTT·웹훅 등)·기간 재처리는 내지 않는다.</li>
 *   <li>모양: {@code capabilities}는 비운다(pipeline은 기능 상태를 모른다) = "업링크가 있었다"만 알린다. action은 빈 보고를 상태 쌍·버전에
 *       반영하지 않고 대기 다운링크만 보낸다. {@code version}은 프레임 카운터(fCnt, 없으면 수신 시각 밀리초), {@code reportedAt}은 수신 시각,
 *       {@code virtual=false}.</li>
 *   <li>멱등: messageId는 표준 메시지 messageId에서 정해진다(같은 원본을 다시 읽어 다시 내도 같은 ID).</li>
 * </ul>
 */
public final class LoRaWanUplinkSignal {

    /** ChirpStack v4 MQTT 통합 이벤트 토픽({@code up}·{@code join}·{@code status}·{@code ack}·{@code txack}·{@code log}·{@code location}) */
    static final Pattern CHIRPSTACK_EVENT = Pattern.compile("(^|.*/)application/[^/]+/device/[^/]+/event/([^/]+)$");

    private LoRaWanUplinkSignal() {
    }

    /**
     * @param canonical 실시간 처리로 저장·발행한 표준 메시지
     * @param topic     원본 토픽(ingress가 받은 토픽)
     * @return 낼 신호. LoRaWAN 업링크가 아니거나 대상 기기가 아니면 빈 값
     */
    public static Optional<DeviceStateReported> from(CanonicalTelemetry canonical, String topic) {
        if (canonical == null || canonical.virtual() || canonical.deviceId() <= 0
                || canonical.deviceStatus() != CanonicalTelemetry.DeviceStatus.ACTIVE || !isLoRaWan(canonical, topic)) {
            return Optional.empty();
        }
        Long fCnt = canonical.link() == null ? null : canonical.link().frameCounter();
        long version = fCnt != null && fCnt >= 0 ? fCnt : canonical.receivedAt().toEpochMilli();
        return Optional.of(new DeviceStateReported(canonical.deviceId(), version, Map.of(), canonical.receivedAt(), false));
    }

    /** 신호의 messageId: 표준 메시지 messageId에서 정해진다 */
    public static UUID messageId(CanonicalTelemetry canonical) {
        return UUID.nameUUIDFromBytes(("uplink:" + canonical.organizationId() + ":" + canonical.messageId())
                .getBytes(StandardCharsets.UTF_8));
    }

    /** ChirpStack 이벤트 토픽이면 {@code event/up}만, 토픽이 없거나 다른 모양(웹훅 등)이면 디코더가 {@code chirpstack-v4}인지로 판정 */
    static boolean isLoRaWan(CanonicalTelemetry canonical, String topic) {
        if (topic != null) {
            java.util.regex.Matcher m = CHIRPSTACK_EVENT.matcher(topic);
            if (m.matches()) {
                return "up".equals(m.group(2));
            }
        }
        CanonicalTelemetry.DecoderRef decoder = canonical.meta() == null ? null : canonical.meta().decoder();
        return decoder != null && DecoderKeys.CHIRPSTACK_V4.equals(decoder.key());
    }
}
