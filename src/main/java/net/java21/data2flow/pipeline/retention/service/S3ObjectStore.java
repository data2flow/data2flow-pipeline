package net.java21.data2flow.pipeline.retention.service;

import net.java21.data2flow.pipeline.common.PipelineProperties;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;

/**
 * S3 호환 오브젝트 저장소 클라이언트(콜드 보관, TSD-05.02). s3 운영은 기존 {@code storage.java21.net}, 설치형 번들은 SeaweedFS
 * (design/oss-stack.md: MinIO는 서비스로만 사용). AWS SDK 대신 표준 HTTP + 서명 v4(경로 방식 {@code /bucket/key})로 PUT·GET만 한다
 * (의존성·이미지 크기를 늘리지 않게). 접근 키는 환경변수(k8s Secret)로 받는다.
 */
public class S3ObjectStore implements ObjectStore {

    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private final PipelineProperties.Archive settings;
    private final String endpoint;
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(5)).build();
    private final Clock clock;

    public S3ObjectStore(PipelineProperties.Archive settings, Clock clock) {
        this.settings = settings;
        this.endpoint = settings.endpoint().endsWith("/") ? settings.endpoint().substring(0, settings.endpoint().length() - 1)
                : settings.endpoint();
        this.clock = clock;
    }

    @Override
    public void put(String key, byte[] body, String contentType) {
        HttpResponse<byte[]> r = send("PUT", "/" + settings.bucket() + "/" + encodePath(key), body, contentType);
        if (r.statusCode() / 100 != 2) {
            throw new ObjectStoreException("PUT " + key + " → " + r.statusCode() + " " + new String(r.body(), StandardCharsets.UTF_8));
        }
    }

    @Override
    public byte[] get(String key) {
        HttpResponse<byte[]> r = send("GET", "/" + settings.bucket() + "/" + encodePath(key), null, null);
        if (r.statusCode() / 100 != 2) {
            throw new ObjectStoreException("GET " + key + " → " + r.statusCode());
        }
        return r.body();
    }

    /** 버킷 만들기(시험·설치형 번들 초기화용. 운영 버킷은 저장소 관리자가 만든다) */
    public void createBucket() {
        HttpResponse<byte[]> r = send("PUT", "/" + settings.bucket(), new byte[0], null);
        if (r.statusCode() / 100 != 2 && r.statusCode() != 409) {
            throw new ObjectStoreException("PUT bucket → " + r.statusCode() + " " + new String(r.body(), StandardCharsets.UTF_8));
        }
    }

    private HttpResponse<byte[]> send(String method, String path, byte[] body, String contentType) {
        byte[] payload = body == null ? new byte[0] : body;
        String payloadHash = hex(sha256(payload));
        java.time.Instant now = clock.instant();
        String amzDate = AMZ_DATE.format(now);
        String date = DATE.format(now);
        URI uri = URI.create(endpoint + path);
        String host = uri.getPort() > 0 ? uri.getHost() + ":" + uri.getPort() : uri.getHost();
        String canonicalHeaders = "host:" + host + "\n" + "x-amz-content-sha256:" + payloadHash + "\n" + "x-amz-date:" + amzDate + "\n";
        String signedHeaders = "host;x-amz-content-sha256;x-amz-date";
        String canonicalRequest = method + "\n" + uri.getRawPath() + "\n\n" + canonicalHeaders + "\n" + signedHeaders + "\n"
                + payloadHash;
        String scope = date + "/" + settings.region() + "/s3/aws4_request";
        String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n"
                + hex(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));
        byte[] key = hmac(("AWS4" + nullToEmpty(settings.secretKey())).getBytes(StandardCharsets.UTF_8), date);
        key = hmac(key, settings.region());
        key = hmac(key, "s3");
        key = hmac(key, "aws4_request");
        String signature = hex(hmac(key, stringToSign));
        HttpRequest.Builder b = HttpRequest.newBuilder(uri).timeout(Duration.ofMinutes(2))
                .header("x-amz-date", amzDate)
                .header("x-amz-content-sha256", payloadHash)
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=" + nullToEmpty(settings.accessKey()) + "/" + scope
                        + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature);
        if (contentType != null) {
            b.header("Content-Type", contentType);
        }
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(payload));
        try {
            return http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException e) {
            throw new ObjectStoreException("오브젝트 저장소에 연결할 수 없습니다: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ObjectStoreException("오브젝트 저장소 호출이 중단되었습니다");
        }
    }

    static String encodePath(String key) {
        StringBuilder out = new StringBuilder();
        for (String segment : key.split("/", -1)) {
            if (!out.isEmpty()) {
                out.append('/');
            }
            out.append(URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20").replace("*", "%2A")
                    .replace("%7E", "~"));
        }
        return out.toString();
    }

    public static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] hmac(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
