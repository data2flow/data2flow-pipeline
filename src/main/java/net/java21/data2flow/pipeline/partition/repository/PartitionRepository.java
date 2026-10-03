package net.java21.data2flow.pipeline.partition.repository;

import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 파티션 관리 DDL과 {@code partition_registries}(ADR-019, BR-TSD-01·02). 스키마 소유자 pipeline만 쓴다. 이름은 코드가 만든
 * 형식({@code telemetry_y2026m10})만 받아 SQL 주입이 없다.
 */
@Repository
@OrganizationScopeExempt("스키마 전체의 파티션 관리(조직 무관)")
public class PartitionRepository {

    private static final String SCHEMA = "data2flow_pipeline";
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{1,62}");

    private final JdbcClient jdbc;

    public PartitionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public boolean exists(String table) {
        check(table);
        return jdbc.sql("SELECT to_regclass(:name) IS NOT NULL").param("name", SCHEMA + "." + table)
                .query(Boolean.class).single();
    }

    /** 범위 파티션을 만든다. DEFAULT에 그 범위의 행이 있으면 실패한다(호출하는 쪽이 옮기기로 처리) */
    public void createPartition(String parent, String partition, Instant from, Instant to) {
        check(parent);
        check(partition);
        jdbc.sql("CREATE TABLE IF NOT EXISTS " + SCHEMA + "." + partition + " PARTITION OF " + SCHEMA + "." + parent
                + " FOR VALUES FROM ('" + from + "') TO ('" + to + "')").update();
    }

    /**
     * DEFAULT 파티션에 범위 행이 있을 때: DEFAULT를 떼고 → 새 파티션을 만들고 → 그 범위 행을 옮긴 뒤 → DEFAULT를 다시 붙인다.
     * 같은 트랜잭션에서 실행해야 한다(호출하는 쪽 책임).
     *
     * @param timeColumn 파티션 키 열(time·bucket·received_at)
     * @return 옮긴 행 수
     */
    public int createMovingDefaultRows(String parent, String partition, String timeColumn, Instant from, Instant to,
                                       boolean identity) {
        check(parent);
        check(partition);
        check(timeColumn);
        String def = SCHEMA + "." + parent + "_default";
        String parentName = SCHEMA + "." + parent;
        jdbc.sql("ALTER TABLE " + parentName + " DETACH PARTITION " + def).update();
        createPartition(parent, partition, from, to);
        int moved = jdbc.sql("INSERT INTO " + parentName + (identity ? " OVERRIDING SYSTEM VALUE" : "")
                        + " SELECT * FROM " + def + " WHERE " + timeColumn + " >= :from AND " + timeColumn + " < :to")
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).update();
        jdbc.sql("DELETE FROM " + def + " WHERE " + timeColumn + " >= :from AND " + timeColumn + " < :to")
                .param("from", Timestamp.from(from)).param("to", Timestamp.from(to)).update();
        jdbc.sql("ALTER TABLE " + parentName + " ATTACH PARTITION " + def + " DEFAULT").update();
        return moved;
    }

    public long countDefaultRows(String parent) {
        check(parent);
        return jdbc.sql("SELECT count(*) FROM " + SCHEMA + "." + parent + "_default").query(Long.class).single();
    }

    public void setLockTimeout(long millis) {
        jdbc.sql("SET LOCAL lock_timeout = '" + Math.max(1, millis) + "ms'").update();
    }

    /** 보관이 지난 파티션을 떼고 지운다(BR-TSD-02: DEFAULT가 있는 부모라 CONCURRENTLY 없이) */
    public void detachAndDrop(String parent, String partition) {
        check(parent);
        check(partition);
        jdbc.sql("ALTER TABLE " + SCHEMA + "." + parent + " DETACH PARTITION " + SCHEMA + "." + partition).update();
        jdbc.sql("DROP TABLE IF EXISTS " + SCHEMA + "." + partition).update();
    }

    /** 부모 테이블의 날짜 파티션 이름(DEFAULT 제외) */
    public List<String> listPartitions(String parent) {
        check(parent);
        return jdbc.sql("""
                        SELECT c.relname FROM pg_inherits i
                          JOIN pg_class c ON c.oid = i.inhrelid
                          JOIN pg_class p ON p.oid = i.inhparent
                          JOIN pg_namespace n ON n.oid = p.relnamespace
                         WHERE n.nspname = :schema AND p.relname = :parent AND c.relname <> :def
                         ORDER BY c.relname""")
                .param("schema", SCHEMA).param("parent", parent).param("def", parent + "_default")
                .query(String.class).list();
    }

    public void register(String partition, String table, Instant from, Instant to, Instant now) {
        jdbc.sql("""
                        INSERT INTO data2flow_pipeline.partition_registries (partition_name, table_name, range_from, range_to,
                            state, updated_at) VALUES (:name, :table, :from, :to, 'CREATED', :now)
                        ON CONFLICT (partition_name) DO UPDATE SET state = 'CREATED', updated_at = EXCLUDED.updated_at
                        WHERE partition_registries.state = 'DROPPED'""")
                .param("name", partition).param("table", table).param("from", Timestamp.from(from))
                .param("to", Timestamp.from(to)).param("now", Timestamp.from(now)).update();
    }

    public void markDropped(String partition, Instant now) {
        jdbc.sql("UPDATE data2flow_pipeline.partition_registries SET state = 'DROPPED', updated_at = :now WHERE partition_name = :name")
                .param("now", Timestamp.from(now)).param("name", partition).update();
    }

    private static void check(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("잘못된 테이블 이름: " + name);
        }
    }
}
