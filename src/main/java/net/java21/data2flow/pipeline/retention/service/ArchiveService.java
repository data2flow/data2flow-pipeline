package net.java21.data2flow.pipeline.retention.service;

import net.java21.data2flow.pipeline.device.service.CoreDirectory;
import net.java21.data2flow.pipeline.retention.domain.ParquetWriter;
import net.java21.data2flow.pipeline.retention.repository.RetentionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

/**
 * 콜드 보관(TSD-05.02, BR-TSD-07·18): 조직의 한 달 원본을 Parquet(zstd)으로 만들어 오브젝트 저장소에 올리고, 다시 내려받아 체크섬
 * (SHA-256)이 같은지 확인한 뒤 core 파일 목록에 등록한다(API-TSD-61). 이 셋이 모두 끝나야 원본을 지울 수 있다. 복원은 core 가져오기
 * 작업(BR-TSD-15)으로 한다.
 */
public class ArchiveService {

    private static final Logger log = LoggerFactory.getLogger(ArchiveService.class);
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);
    static final List<ParquetWriter.Column> TELEMETRY_COLUMNS = List.of(
            new ParquetWriter.Column("device_id", ParquetWriter.ColumnType.INT64, false),
            new ParquetWriter.Column("metric_key", ParquetWriter.ColumnType.STRING, false),
            new ParquetWriter.Column("time", ParquetWriter.ColumnType.TIMESTAMP_MICROS, false),
            new ParquetWriter.Column("organization_id", ParquetWriter.ColumnType.INT64, false),
            new ParquetWriter.Column("value", ParquetWriter.ColumnType.DOUBLE, false),
            new ParquetWriter.Column("quality", ParquetWriter.ColumnType.INT32, false),
            new ParquetWriter.Column("flags", ParquetWriter.ColumnType.INT32, false),
            new ParquetWriter.Column("is_virtual", ParquetWriter.ColumnType.BOOLEAN, false),
            new ParquetWriter.Column("received_at", ParquetWriter.ColumnType.TIMESTAMP_MICROS, false),
            new ParquetWriter.Column("raw_message_id", ParquetWriter.ColumnType.INT64, true));

    private final ObjectStore store;
    private final CoreDirectory core;
    private final RetentionRepository repository;
    private final String prefix;

    public ArchiveService(ObjectStore store, CoreDirectory core, RetentionRepository repository, String prefix) {
        this.store = store;
        this.core = core;
        this.repository = repository;
        this.prefix = prefix;
    }

    /**
     * 조직의 [from, to) 원본을 콜드 보관한다. 이미 등록한 범위면 다시 하지 않는다.
     *
     * @return 올린 파일. 옮길 행이 없으면 빈 값
     * @throws RuntimeException 저장소·체크섬·core 등록 실패(원본을 지우면 안 됨)
     */
    public Optional<Archived> archiveTelemetry(String table, long organizationId, Instant from, Instant to, String excludeSql,
                                               Object[] excludeArgs) {
        if (repository.existsRegisteredArchive(organizationId, "TELEMETRY", from, to)) {
            return Optional.of(new Archived(objectKey(organizationId, from, to), 0, 0, null, 0, true));
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        long rows;
        try (ParquetWriter writer = new ParquetWriter(buffer, TELEMETRY_COLUMNS, 100_000)) {
            repository.streamTelemetry(table, organizationId, from, to, excludeSql, excludeArgs, rs -> {
                long raw = rs.getLong("raw_message_id");
                Long rawId = rs.wasNull() ? null : raw;
                writer.write(rs.getLong("device_id"), rs.getString("metric_key"), rs.getTimestamp("time").toInstant(),
                        rs.getLong("organization_id"), rs.getDouble("value"), (int) rs.getShort("quality"),
                        (int) rs.getShort("flags"), rs.getBoolean("is_virtual"), rs.getTimestamp("received_at").toInstant(),
                        rawId);
            });
            rows = writer.rows();
        }
        if (rows == 0) {
            return Optional.empty();
        }
        byte[] file = buffer.toByteArray();
        String checksum = S3ObjectStore.hex(S3ObjectStore.sha256(file));
        String key = objectKey(organizationId, from, to);
        long sourceBytes = repository.estimateBytes(table, organizationId, from, to);
        store.put(key, file, "application/vnd.apache.parquet");
        byte[] back = store.get(key);
        if (!MessageDigest.isEqual(S3ObjectStore.sha256(back), S3ObjectStore.sha256(file))) {
            throw new ObjectStore.ObjectStoreException("체크섬이 다릅니다: " + key);
        }
        repository.upsertArchive(organizationId, "TELEMETRY", from, to, key, rows, file.length, sourceBytes, checksum, false);
        core.registerArchive(new CoreDirectory.ArchiveFile(organizationId, "TELEMETRY", from, to, key, rows, file.length,
                checksum));
        repository.upsertArchive(organizationId, "TELEMETRY", from, to, key, rows, file.length, sourceBytes, checksum, true);
        log.info("콜드 보관: {} ({}행, {}바이트, 원본 약 {}바이트)", key, rows, file.length, sourceBytes);
        return Optional.of(new Archived(key, rows, file.length, checksum, sourceBytes, false));
    }

    String objectKey(long organizationId, Instant from, Instant to) {
        String year = DateTimeFormatter.ofPattern("yyyy").withZone(ZoneOffset.UTC).format(from);
        return prefix + "/org-" + organizationId + "/telemetry/" + year + "/telemetry-" + organizationId + "-" + DAY.format(from)
                + "-" + DAY.format(to) + ".parquet";
    }

    /**
     * @param objectKey   객체 키
     * @param rows        행 수
     * @param bytes       파일 크기
     * @param checksum    SHA-256
     * @param sourceBytes 같은 범위 DB 원본 크기 추정
     * @param already     이미 등록되어 있던 범위
     */
    public record Archived(String objectKey, long rows, long bytes, String checksum, long sourceBytes, boolean already) {

        public Double ratio() {
            return sourceBytes <= 0 ? null : (double) bytes / sourceBytes;
        }
    }
}
