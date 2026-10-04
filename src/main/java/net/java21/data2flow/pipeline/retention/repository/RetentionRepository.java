package net.java21.data2flow.pipeline.retention.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 보관 정리·콜드 보관·정렬 재작성용 SQL(TSD-02.01·02.03·05.01·05.02, BR-TSD-02·07·18). 표·열 이름은 코드가 정한 목록만 받는다.
 * 조건은 언제나 조직을 건다(조직마다 보관 기간이 다르다).
 */
@Repository
public class RetentionRepository {

    private static final String SCHEMA = "data2flow_pipeline";
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{1,62}");

    /** 표 → 행을 가리키는 키 열 */
    public static final Map<String, String> KEYS = Map.of(
            "telemetry", "device_id, metric_key, time",
            "telemetry_long", "device_id, metric_key, time",
            "telemetry_1m", "device_id, metric_key, bucket",
            "telemetry_1h", "device_id, metric_key, bucket",
            "telemetry_1d", "device_id, metric_key, bucket",
            "link_qualities", "device_id, gateway_eui, time",
            "raw_messages", "id, received_at");

    private final JdbcTemplate jdbc;
    private final JdbcTemplate streaming;

    public RetentionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.streaming = new JdbcTemplate(jdbc.getDataSource());
        this.streaming.setFetchSize(5_000);
    }

    /** 표(또는 파티션)에 행이 있는 조직 */
    @OrganizationScopeExempt("보관 정리 대상 조직 찾기")
    public List<Long> listOrganizations(String table) {
        check(table);
        return jdbc.queryForList("SELECT DISTINCT organization_id FROM " + SCHEMA + "." + table, Long.class);
    }

    /** 조직별 행 수(파티션을 통째로 지우기 전 retention.purged 행 수) */
    @OrganizationScopeExempt("파티션 전체 집계")
    public Map<Long, Long> countByOrganization(String table) {
        check(table);
        Map<Long, Long> out = new java.util.LinkedHashMap<>();
        jdbc.query("SELECT organization_id, count(*) FROM " + SCHEMA + "." + table + " GROUP BY organization_id",
                (RowCallbackHandler) rs -> out.put(rs.getLong(1), rs.getLong(2)));
        return out;
    }

    /**
     * 조직의 [시간 열 < before] 행을 배치로 지운다(BR-TSD-02: 1만 행씩). {@code extraSql}은 코드가 만든 조건(측정 키·기기 목록)이다.
     *
     * @return 지운 행 수
     */
    public long deleteBefore(String table, String timeColumn, long organizationId, Instant before, String extraSql,
                             Object[] extraArgs, int batch) {
        check(table);
        check(timeColumn);
        String keys = KEYS.get(table);
        long total = 0;
        while (true) {
            List<Object> args = new ArrayList<>(List.of(organizationId, Timestamp.from(before)));
            args.addAll(List.of(extraArgs));
            args.add(batch);
            int n = jdbc.update("DELETE FROM " + SCHEMA + "." + table + " WHERE (" + keys + ") IN (SELECT " + keys + " FROM "
                    + SCHEMA + "." + table + " WHERE organization_id = ? AND " + timeColumn + " < ?"
                    + (extraSql == null ? "" : " AND " + extraSql) + " LIMIT ?)", args.toArray());
            total += n;
            if (n < batch) {
                return total;
            }
        }
    }

    /** 지울 행 수 미리 보기(BR-TSD-03 근거) */
    public long countBefore(String table, String timeColumn, long organizationId, Instant before, String extraSql,
                            Object[] extraArgs) {
        check(table);
        check(timeColumn);
        List<Object> args = new ArrayList<>(List.of(organizationId, Timestamp.from(before)));
        args.addAll(List.of(extraArgs));
        Long n = jdbc.queryForObject("SELECT count(*) FROM " + SCHEMA + "." + table + " WHERE organization_id = ? AND "
                + timeColumn + " < ?" + (extraSql == null ? "" : " AND " + extraSql), Long.class, args.toArray());
        return n == null ? 0 : n;
    }

    /** 연장 보관으로 옮기기(파티션을 지우기 전, BR-TSD-02). 옮긴 행 수 */
    public int insertIntoLong(String partition, long organizationId, String conditionSql, Object[] args) {
        check(partition);
        List<Object> all = new ArrayList<>(List.of(organizationId));
        all.addAll(List.of(args));
        return jdbc.update("INSERT INTO " + SCHEMA + ".telemetry_long SELECT * FROM " + SCHEMA + "." + partition
                + " WHERE organization_id = ? AND (" + conditionSql + ") ON CONFLICT DO NOTHING", all.toArray());
    }

    /** 원본 행을 순서대로 읽는다(콜드 보관 내보내기). 조건은 코드가 만든 것 */
    public void streamTelemetry(String table, long organizationId, Instant from, Instant to, String excludeSql,
                                Object[] excludeArgs, RowCallbackHandler handler) {
        check(table);
        List<Object> args = new ArrayList<>(List.of(organizationId, Timestamp.from(from), Timestamp.from(to)));
        args.addAll(List.of(excludeArgs));
        streaming.query("SELECT device_id, metric_key, time, organization_id, value, quality, flags, is_virtual, received_at, "
                + "raw_message_id FROM " + SCHEMA + "." + table + " WHERE organization_id = ? AND time >= ? AND time < ?"
                + (excludeSql == null ? "" : " AND NOT (" + excludeSql + ")") + " ORDER BY device_id, metric_key, time",
                handler, args.toArray());
    }

    /** 표 크기(바이트, 인덱스 포함) */
    @OrganizationScopeExempt("파티션 크기")
    public long sizeOf(String table) {
        check(table);
        Long n = jdbc.queryForObject("SELECT pg_total_relation_size(to_regclass(?))", Long.class, SCHEMA + "." + table);
        return n == null ? 0 : n;
    }

    /** 조직의 [from, to) 원본이 차지하는 크기 추정(행 수 × 평균 행 크기) */
    public long estimateBytes(String table, long organizationId, Instant from, Instant to) {
        check(table);
        Long rows = jdbc.queryForObject("SELECT count(*) FROM " + SCHEMA + "." + table
                + " WHERE organization_id = ? AND time >= ? AND time < ?", Long.class, organizationId, Timestamp.from(from),
                Timestamp.from(to));
        Long total = jdbc.queryForObject("SELECT count(*) FROM " + SCHEMA + "." + table, Long.class);
        long size = sizeOf(table);
        return rows == null || total == null || total == 0 ? 0 : size * rows / total;
    }

    /** 이미 올리고 등록한 콜드 보관 파일인가 */
    public boolean existsRegisteredArchive(long organizationId, String dataClass, Instant from, Instant to) {
        Boolean b = jdbc.queryForObject("""
                SELECT EXISTS (SELECT 1 FROM data2flow_pipeline.archive_exports WHERE organization_id = ? AND data_class = ?
                                AND range_from = ? AND range_to = ? AND registered)""", Boolean.class, organizationId,
                dataClass, Timestamp.from(from), Timestamp.from(to));
        return Boolean.TRUE.equals(b);
    }

    public void upsertArchive(long organizationId, String dataClass, Instant from, Instant to, String objectKey, long rows,
                              long bytes, long sourceBytes, String checksum, boolean registered) {
        jdbc.update("""
                INSERT INTO data2flow_pipeline.archive_exports (organization_id, data_class, range_from, range_to, object_key,
                    rows_count, bytes, source_bytes, checksum, registered)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (object_key) DO UPDATE SET rows_count = EXCLUDED.rows_count, bytes = EXCLUDED.bytes,
                    source_bytes = EXCLUDED.source_bytes, checksum = EXCLUDED.checksum, registered = EXCLUDED.registered""",
                organizationId, dataClass, Timestamp.from(from), Timestamp.from(to), objectKey, rows, bytes, sourceBytes,
                checksum, registered);
    }

    /** 파티션 등록 정보에 콜드 보관 크기·비율 기록(BR-TSD-07) */
    @OrganizationScopeExempt("파티션 등록 정보")
    public void updateRegistryArchive(String partition, String state, long archiveBytes, Double ratio, Instant now) {
        jdbc.update("""
                UPDATE data2flow_pipeline.partition_registries SET state = ?, archive_bytes = coalesce(archive_bytes, 0) + ?,
                       archive_ratio = coalesce(?, archive_ratio), updated_at = ?
                 WHERE partition_name = ?""", state, archiveBytes, ratio, Timestamp.from(now), partition);
    }

    /** 정렬 재작성 대상: 범위 끝이 before보다 이른 아직 재작성하지 않은 원본 파티션(오래된 것부터) */
    @OrganizationScopeExempt("파티션 관리")
    public List<String[]> listUnsortedPartitions(String table, Instant before) {
        return jdbc.query("""
                SELECT partition_name, range_from, range_to FROM data2flow_pipeline.partition_registries
                 WHERE table_name = ? AND state = 'CREATED' AND sorted_at IS NULL AND range_to <= ?
                 ORDER BY range_from""", (rs, n) -> new String[]{rs.getString(1),
                rs.getTimestamp(2).toInstant().toString(), rs.getTimestamp(3).toInstant().toString()},
                table, Timestamp.from(before));
    }

    /**
     * 정렬 재작성(BR-TSD-07): 같은 트랜잭션 안에서 파티션 쓰기를 막고(lock_timeout) (device_id, time) 순서로 새 표에 옮긴 뒤 바꿔 끼운다.
     * 호출하는 쪽이 트랜잭션을 연다.
     */
    @OrganizationScopeExempt("파티션 관리")
    public void rewriteSorted(String parent, String partition, Instant from, Instant to, long lockTimeoutMillis, Instant now) {
        check(parent);
        check(partition);
        String tmp = (partition.length() > 55 ? partition.substring(0, 55) : partition) + "_sorted";
        check(tmp);
        jdbc.execute("SET LOCAL lock_timeout = '" + Math.max(1, lockTimeoutMillis) + "ms'");
        jdbc.execute("LOCK TABLE " + SCHEMA + "." + partition + " IN SHARE MODE");
        jdbc.execute("DROP TABLE IF EXISTS " + SCHEMA + "." + tmp);
        jdbc.execute("CREATE TABLE " + SCHEMA + "." + tmp + " (LIKE " + SCHEMA + "." + partition
                + " INCLUDING DEFAULTS INCLUDING CONSTRAINTS)");
        jdbc.execute("INSERT INTO " + SCHEMA + "." + tmp + " SELECT * FROM " + SCHEMA + "." + partition
                + " ORDER BY device_id, time, metric_key");
        jdbc.execute("ALTER TABLE " + SCHEMA + "." + parent + " DETACH PARTITION " + SCHEMA + "." + partition);
        jdbc.execute("DROP TABLE " + SCHEMA + "." + partition);
        jdbc.execute("ALTER TABLE " + SCHEMA + "." + tmp + " RENAME TO " + partition);
        jdbc.execute("ALTER TABLE " + SCHEMA + "." + parent + " ATTACH PARTITION " + SCHEMA + "." + partition
                + " FOR VALUES FROM ('" + from + "') TO ('" + to + "')");
        jdbc.update("""
                UPDATE data2flow_pipeline.partition_registries SET state = 'COMPRESSED', sorted_at = ?, bytes = ?,
                       rows_estimate = (SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass(?)), updated_at = ?
                 WHERE partition_name = ?""", Timestamp.from(now), sizeOf(partition), SCHEMA + "." + partition,
                Timestamp.from(now), partition);
    }

    /** 파티션 등록 정보의 살아 있는 파티션과 범위(오래된 것부터) */
    @OrganizationScopeExempt("파티션 관리")
    public List<PartitionRange> listPartitionRanges(String table) {
        return jdbc.query("""
                SELECT partition_name, range_from, range_to FROM data2flow_pipeline.partition_registries
                 WHERE table_name = ? AND state <> 'DROPPED' AND to_regclass('data2flow_pipeline.' || partition_name) IS NOT NULL
                 ORDER BY range_from""", (rs, n) -> new PartitionRange(rs.getString(1), rs.getTimestamp(2).toInstant(),
                rs.getTimestamp(3).toInstant()), table);
    }

    /** 표(파티션)의 행 수 */
    @OrganizationScopeExempt("파티션이 비었는지")
    public long countRows(String table) {
        check(table);
        Long n = jdbc.queryForObject("SELECT count(*) FROM " + SCHEMA + "." + table, Long.class);
        return n == null ? 0 : n;
    }

    /** 조직의 조건 행 지우기(연장 보관으로 옮긴 뒤). 지운 행 수 */
    public int deleteMatching(String table, long organizationId, String conditionSql, Object[] args) {
        check(table);
        List<Object> all = new ArrayList<>(List.of(organizationId));
        all.addAll(List.of(args));
        return jdbc.update("DELETE FROM " + SCHEMA + "." + table + " WHERE organization_id = ? AND (" + conditionSql + ")",
                all.toArray());
    }

    /** 보관 정리 대상 조직(기기 상태가 있는 조직) */
    @OrganizationScopeExempt("보관 정리 대상 조직")
    public List<Long> listKnownOrganizations() {
        return jdbc.queryForList("SELECT DISTINCT organization_id FROM data2flow_pipeline.device_state", Long.class);
    }

    /** 파티션 범위 */
    public record PartitionRange(String name, Instant from, Instant to) {
    }

    private static void check(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("잘못된 이름: " + name);
        }
    }
}
