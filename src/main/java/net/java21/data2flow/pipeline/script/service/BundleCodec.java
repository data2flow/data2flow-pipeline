package net.java21.data2flow.pipeline.script.service;

import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.pipeline.script.domain.RuntimeBundle;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실행 번들을 JSON으로 고정·복원한다(재처리 작업 {@code reprocess_jobs.pinned_bundle}, BR-ING-12). 다른 인스턴스가 작업을 넘겨받아도
 * 같은 버전으로 잇기 위해 DB에 둔다.
 */
public final class BundleCodec {

    private static final JsonMapper MAPPER = MessageCodec.newMapper();

    private BundleCodec() {
    }

    public static String write(RuntimeBundle bundle) {
        return MAPPER.writeValueAsString(bundle);
    }

    public static RuntimeBundle read(String json) {
        if (json == null || json.isBlank()) {
            return RuntimeBundle.EMPTY;
        }
        return MAPPER.readValue(json, RuntimeBundle.class);
    }
}
