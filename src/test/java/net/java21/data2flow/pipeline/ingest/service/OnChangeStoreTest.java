package net.java21.data2flow.pipeline.ingest.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** TSD-05.03 상태형 측정 항목 변화 시에만 저장(BR-TSD-17) */
class OnChangeStoreTest {

    private static JsonNode prev(double v, String t, String s) {
        return MessageCodec.newMapper().readTree("{\"v\":" + v + ",\"t\":\"" + t + "\"" + (s == null ? "" : ",\"s\":\"" + s + "\"")
                + "}");
    }

    @Test
    @DisplayName("[TSD-05.03][BR-TSD-17] TC-TSD-132 직전 값과 같으면 저장 안 함, 다르면 변화로 저장, 같아도 1시간마다 하트비트 저장")
    void decisions() {
        Instant t = Instant.parse("2026-10-03T01:00:00Z");
        assertThat(IngestStore.onChange(null, 1, t)).isEqualTo(IngestStore.OnChange.CHANGED);
        assertThat(IngestStore.onChange(prev(1, "2026-10-03T00:59:00Z", "2026-10-03T00:30:00Z"), 1, t))
                .isEqualTo(IngestStore.OnChange.SKIP);
        assertThat(IngestStore.onChange(prev(1, "2026-10-03T00:59:00Z", "2026-10-03T00:30:00Z"), 0, t))
                .isEqualTo(IngestStore.OnChange.CHANGED);
        assertThat(IngestStore.onChange(prev(1, "2026-10-03T00:59:00Z", "2026-10-03T00:00:00Z"), 1, t))
                .as("1시간 지남").isEqualTo(IngestStore.OnChange.HEARTBEAT);
        assertThat(IngestStore.onChange(prev(1, "2026-10-03T00:59:00Z", null), 1, t)).isEqualTo(IngestStore.OnChange.HEARTBEAT);
        assertThat(IngestStore.onChange(prev(1, "2026-10-03T02:00:00Z", "2026-10-03T02:00:00Z"), 1, t))
                .as("순서가 바뀐 값은 저장").isEqualTo(IngestStore.OnChange.HEARTBEAT);
        assertThat(IngestStore.validZone("Asia/Seoul")).isEqualTo("Asia/Seoul");
        assertThat(IngestStore.validZone("+09:00")).isNull();
        assertThat(IngestStore.validZone(" ")).isNull();
        assertThat(IngestStore.storedSignature("VERIFIED")).isEqualTo("VERIFIED");
        assertThat(IngestStore.storedSignature("WEIRD")).isNull();
    }
}
