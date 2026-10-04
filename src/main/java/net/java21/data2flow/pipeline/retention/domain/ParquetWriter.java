package net.java21.data2flow.pipeline.retention.domain;

import com.github.luben.zstd.Zstd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 콜드 보관용 최소 Parquet 쓰기(TSD-05.02, BR-TSD-07·18). 평평한 스키마(중첩 없음)만, 값은 PLAIN 인코딩, 페이지는 zstd 압축,
 * 열마다 행 묶음(row group)당 데이터 페이지 하나다. Hadoop 의존성(Apache parquet-java는 hadoop-common을 끌어온다 — 이미지 크기와
 * 취약점 때문에) 없이 파일 형식(parquet-format, Thrift compact 메타데이터)만 직접 쓴다. 읽기는 표준 도구(pyarrow·DuckDB·Spark·
 * parquet-java)로 한다(가져오기 복원은 core-api, BR-TSD-15).
 *
 * <p>지원 형식: INT32, INT64, DOUBLE, BOOLEAN, BYTE_ARRAY(UTF8), INT64 TIMESTAMP_MICROS(UTC). 빈 값은 OPTIONAL 열만.
 */
public final class ParquetWriter implements AutoCloseable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final int CODEC_ZSTD = 6;

    /** 열 종류 */
    public enum ColumnType {
        INT32(1, -1), INT64(2, -1), DOUBLE(5, -1), BOOLEAN(0, -1), STRING(6, 0), TIMESTAMP_MICROS(2, 10);

        final int physical;
        final int converted;

        ColumnType(int physical, int converted) {
            this.physical = physical;
            this.converted = converted;
        }
    }

    /** 열 정의 */
    public record Column(String name, ColumnType type, boolean optional) {
    }

    private final CountingStream out;
    private final List<Column> columns;
    private final int rowGroupSize;
    private final List<byte[]> rowGroups = new ArrayList<>();
    private final ColumnBuffer[] buffers;
    private long totalRows;
    private int rowsInGroup;
    private boolean closed;

    public ParquetWriter(OutputStream out, List<Column> columns, int rowGroupSize) {
        this.out = new CountingStream(out);
        this.columns = List.copyOf(columns);
        this.rowGroupSize = rowGroupSize;
        this.buffers = new ColumnBuffer[columns.size()];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = new ColumnBuffer(columns.get(i));
        }
        write(MAGIC);
    }

    /** 한 행. 값은 열 순서대로(Long·Integer·Double·Boolean·String·Instant(TIMESTAMP_MICROS), OPTIONAL 열은 null 가능) */
    public void write(Object... values) {
        if (values.length != columns.size()) {
            throw new IllegalArgumentException("열 수가 맞지 않습니다: " + values.length + "/" + columns.size());
        }
        for (int i = 0; i < values.length; i++) {
            buffers[i].add(values[i]);
        }
        rowsInGroup++;
        totalRows++;
        if (rowsInGroup >= rowGroupSize) {
            flushRowGroup();
        }
    }

    public long rows() {
        return totalRows;
    }

    private void flushRowGroup() {
        if (rowsInGroup == 0) {
            return;
        }
        Thrift rowGroup = new Thrift();
        rowGroup.beginList(1, Thrift.STRUCT, columns.size());
        long totalSize = 0;
        for (ColumnBuffer b : buffers) {
            byte[] raw = b.page(rowsInGroup);
            byte[] compressed = Zstd.compress(raw, 3);
            Thrift header = new Thrift();
            header.i32(1, 0);                       // DATA_PAGE
            header.i32(2, raw.length);
            header.i32(3, compressed.length);
            header.beginStruct(5);                  // DataPageHeader
            header.i32(1, rowsInGroup);
            header.i32(2, 0);                       // PLAIN
            header.i32(3, 3);                       // RLE
            header.i32(4, 3);                       // RLE
            header.endStruct();
            header.stop();
            long pageOffset = out.count;
            byte[] headerBytes = header.bytes();
            write(headerBytes);
            write(compressed);
            long uncompressedTotal = headerBytes.length + raw.length;
            long compressedTotal = headerBytes.length + compressed.length;
            totalSize += uncompressedTotal;
            // ColumnChunk
            rowGroup.beginListElement();
            rowGroup.i64(2, pageOffset);
            rowGroup.beginStruct(3);                // ColumnMetaData
            rowGroup.i32(1, b.column.type().physical);
            rowGroup.beginList(2, Thrift.I32, b.column.optional() ? 2 : 1);
            rowGroup.listI32(0);                    // PLAIN
            if (b.column.optional()) {
                rowGroup.listI32(3);                // RLE(정의 수준)
            }
            rowGroup.beginList(3, Thrift.BINARY, 1);
            rowGroup.listString(b.column.name());
            rowGroup.i32(4, CODEC_ZSTD);
            rowGroup.i64(5, rowsInGroup);
            rowGroup.i64(6, uncompressedTotal);
            rowGroup.i64(7, compressedTotal);
            rowGroup.i64(9, pageOffset);
            rowGroup.endStruct();
            rowGroup.endListElement();
            b.reset();
        }
        rowGroup.i64(2, totalSize);
        rowGroup.i64(3, rowsInGroup);
        rowGroups.add(rowGroup.structBody());
        rowsInGroup = 0;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        flushRowGroup();
        Thrift meta = new Thrift();
        meta.i32(1, 1);
        meta.beginList(2, Thrift.STRUCT, columns.size() + 1);
        meta.beginListElement();
        meta.string(4, "schema");
        meta.i32(5, columns.size());
        meta.endListElement();
        for (Column c : columns) {
            meta.beginListElement();
            meta.i32(1, c.type().physical);
            meta.i32(3, c.optional() ? 1 : 0);
            meta.string(4, c.name());
            if (c.type().converted >= 0) {
                meta.i32(6, c.type().converted);
            }
            meta.endListElement();
        }
        meta.i64(3, totalRows);
        meta.beginList(4, Thrift.STRUCT, rowGroups.size());
        for (byte[] rg : rowGroups) {
            meta.rawListStruct(rg);
        }
        meta.string(6, "data2flow-pipeline parquet writer");
        meta.stop();
        byte[] footer = meta.bytes();
        write(footer);
        write(new byte[]{(byte) footer.length, (byte) (footer.length >>> 8), (byte) (footer.length >>> 16),
                (byte) (footer.length >>> 24)});
        write(MAGIC);
        try {
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void write(byte[] bytes) {
        try {
            out.write(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 한 열의 행 묶음 버퍼(PLAIN 값 + 정의 수준) */
    private static final class ColumnBuffer {
        final Column column;
        final ByteArrayOutputStream values = new ByteArrayOutputStream();
        final ByteArrayOutputStream defs = new ByteArrayOutputStream();
        int defBits;
        int defCount;
        int boolBits;
        int boolCount;

        ColumnBuffer(Column column) {
            this.column = column;
        }

        void add(Object v) {
            if (column.optional()) {
                defBits |= (v == null ? 0 : 1) << (defCount % 8);
                defCount++;
                if (defCount % 8 == 0) {
                    defs.write(defBits);
                    defBits = 0;
                }
            } else if (v == null) {
                throw new IllegalArgumentException("필수 열에 빈 값: " + column.name());
            }
            if (v == null) {
                return;
            }
            switch (column.type()) {
                case INT32 -> le(((Number) v).intValue(), 4);
                case INT64 -> le(((Number) v).longValue(), 8);
                case TIMESTAMP_MICROS -> {
                    java.time.Instant t = (java.time.Instant) v;
                    le(Math.addExact(Math.multiplyExact(t.getEpochSecond(), 1_000_000L), t.getNano() / 1_000), 8);
                }
                case DOUBLE -> le(Double.doubleToRawLongBits(((Number) v).doubleValue()), 8);
                case BOOLEAN -> {
                    boolBits |= (Boolean.TRUE.equals(v) ? 1 : 0) << (boolCount % 8);
                    boolCount++;
                    if (boolCount % 8 == 0) {
                        values.write(boolBits);
                        boolBits = 0;
                    }
                }
                case STRING -> {
                    byte[] s = v.toString().getBytes(StandardCharsets.UTF_8);
                    le(s.length, 4);
                    values.writeBytes(s);
                }
            }
        }

        private void le(long v, int bytes) {
            for (int i = 0; i < bytes; i++) {
                values.write((int) (v >>> (8 * i)) & 0xff);
            }
        }

        /** 데이터 페이지 v1 본문: [정의 수준(4바이트 길이 + RLE/비트 묶음)] + 값 */
        byte[] page(int rows) {
            ByteArrayOutputStream page = new ByteArrayOutputStream();
            if (column.optional()) {
                if (defCount % 8 != 0) {
                    defs.write(defBits);
                }
                byte[] packed = defs.toByteArray();
                ByteArrayOutputStream hybrid = new ByteArrayOutputStream();
                writeVarint(hybrid, ((long) packed.length << 1) | 1);   // 비트 묶음 실행: 8개씩 묶음 수
                hybrid.writeBytes(packed);
                byte[] h = hybrid.toByteArray();
                page.write(h.length);
                page.write(h.length >>> 8);
                page.write(h.length >>> 16);
                page.write(h.length >>> 24);
                page.writeBytes(h);
            }
            if (column.type() == ColumnType.BOOLEAN && boolCount % 8 != 0) {
                values.write(boolBits);
            }
            page.writeBytes(values.toByteArray());
            return page.toByteArray();
        }

        void reset() {
            values.reset();
            defs.reset();
            defBits = 0;
            defCount = 0;
            boolBits = 0;
            boolCount = 0;
        }
    }

    static void writeVarint(ByteArrayOutputStream out, long v) {
        long x = v;
        while ((x & ~0x7FL) != 0) {
            out.write((int) ((x & 0x7F) | 0x80));
            x >>>= 7;
        }
        out.write((int) x);
    }

    /** Thrift compact protocol 쓰기(필요한 만큼만) */
    static final class Thrift {
        static final int I32 = 5;
        static final int I64 = 6;
        static final int BINARY = 8;
        static final int STRUCT = 12;

        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        private final java.util.ArrayDeque<Integer> lastIds = new java.util.ArrayDeque<>();
        private int lastId;

        void i32(int id, int v) {
            field(id, I32);
            writeVarint(buf, zigzag(v));
        }

        void i64(int id, long v) {
            field(id, I64);
            writeVarint(buf, zigzag(v));
        }

        void string(int id, String v) {
            field(id, BINARY);
            byte[] b = v.getBytes(StandardCharsets.UTF_8);
            writeVarint(buf, b.length);
            buf.writeBytes(b);
        }

        void beginStruct(int id) {
            field(id, STRUCT);
            lastIds.push(lastId);
            lastId = 0;
        }

        void endStruct() {
            buf.write(0);
            lastId = lastIds.pop();
        }

        void beginList(int id, int elemType, int size) {
            field(id, 9);
            if (size < 15) {
                buf.write((size << 4) | elemType);
            } else {
                buf.write(0xF0 | elemType);
                writeVarint(buf, size);
            }
        }

        void listI32(int v) {
            writeVarint(buf, zigzag(v));
        }

        void listString(String v) {
            byte[] b = v.getBytes(StandardCharsets.UTF_8);
            writeVarint(buf, b.length);
            buf.writeBytes(b);
        }

        void beginListElement() {
            lastIds.push(lastId);
            lastId = 0;
        }

        void endListElement() {
            buf.write(0);
            lastId = lastIds.pop();
        }

        /** 따로 만든 구조체 본문(필드들, 끝 표시 없음)을 리스트 원소로 */
        void rawListStruct(byte[] body) {
            buf.writeBytes(body);
            buf.write(0);
        }

        /** 이 버퍼를 구조체 본문으로(끝 표시 없음) */
        byte[] structBody() {
            return buf.toByteArray();
        }

        void stop() {
            buf.write(0);
        }

        byte[] bytes() {
            return buf.toByteArray();
        }

        private void field(int id, int type) {
            int delta = id - lastId;
            if (delta > 0 && delta <= 15) {
                buf.write((delta << 4) | type);
            } else {
                buf.write(type);
                writeVarint(buf, zigzag(id));
            }
            lastId = id;
        }

        private static long zigzag(long v) {
            return (v << 1) ^ (v >> 63);
        }
    }

    private static final class CountingStream extends OutputStream {
        private final OutputStream delegate;
        long count;

        CountingStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            count += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }
    }
}
