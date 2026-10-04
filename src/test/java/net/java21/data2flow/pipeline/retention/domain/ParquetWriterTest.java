package net.java21.data2flow.pipeline.retention.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TSD-05.02 콜드 보관 Parquet 쓰기. 파일 구조(머리·꼬리 PAR1, 바닥글 길이)와 압축 효과를 본다. 표준 도구로 읽히는지는 구현 때
 * pyarrow로 확인했다(스키마·행 수·빈 값·여러 행 묶음).
 */
class ParquetWriterTest {

    private static final List<ParquetWriter.Column> COLUMNS = List.of(
            new ParquetWriter.Column("device_id", ParquetWriter.ColumnType.INT64, false),
            new ParquetWriter.Column("metric_key", ParquetWriter.ColumnType.STRING, false),
            new ParquetWriter.Column("time", ParquetWriter.ColumnType.TIMESTAMP_MICROS, false),
            new ParquetWriter.Column("value", ParquetWriter.ColumnType.DOUBLE, false),
            new ParquetWriter.Column("quality", ParquetWriter.ColumnType.INT32, false),
            new ParquetWriter.Column("is_virtual", ParquetWriter.ColumnType.BOOLEAN, false),
            new ParquetWriter.Column("raw_message_id", ParquetWriter.ColumnType.INT64, true));

    @Test
    @DisplayName("[TSD-05.02][BR-TSD-18] Parquet 파일 구조: PAR1로 시작·끝, 바닥글 길이, 행 수, zstd로 원래 크기보다 작다")
    void structure() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        int rows = 20_000;
        try (ParquetWriter w = new ParquetWriter(out, COLUMNS, 7_000)) {
            for (int i = 0; i < rows; i++) {
                w.write((long) (i % 5), "temperature", t0.plusSeconds(60L * i), 20 + (i % 50) / 10.0, 0, i % 2 == 0,
                        i % 3 == 0 ? null : (long) i);
            }
            assertThat(w.rows()).isEqualTo(rows);
        }
        byte[] file = out.toByteArray();
        assertThat(new String(Arrays.copyOfRange(file, 0, 4), StandardCharsets.US_ASCII)).isEqualTo("PAR1");
        assertThat(new String(Arrays.copyOfRange(file, file.length - 4, file.length), StandardCharsets.US_ASCII))
                .isEqualTo("PAR1");
        int footer = ByteBuffer.wrap(file, file.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        assertThat(footer).isBetween(100, file.length);
        String meta = new String(Arrays.copyOfRange(file, file.length - 8 - footer, file.length - 8), StandardCharsets.UTF_8);
        assertThat(meta).contains("schema", "device_id", "metric_key", "raw_message_id", "data2flow-pipeline");
        int plain = rows * (8 + 4 + 11 + 8 + 8 + 4 + 8);
        assertThat(file.length).as("압축").isLessThan(plain / 5);
    }

    @Test
    @DisplayName("[TSD-05.02] 열 수가 다르거나 필수 열에 빈 값이면 거부")
    void rejects() {
        ParquetWriter w = new ParquetWriter(new ByteArrayOutputStream(), COLUMNS, 10);
        assertThatThrownBy(() -> w.write(1L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> w.write(null, "k", Instant.EPOCH, 1.0, 0, true, null))
                .isInstanceOf(IllegalArgumentException.class);
        w.close();
        w.close();
    }
}
