package net.java21.data2flow.pipeline.ingest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * API-ING-22 응답 {@code {results:[{id, outcome: OK|FAILED|SKIPPED, errorCode?, previousErrorCode?}], ok, failed, skipped}}.
 * core-api는 previousErrorCode와 errorCode를 비교해 SAME_ERROR·OTHER_ERROR를 만든다(API-ING-07).
 */
public record ReprocessItemsResponse(List<Result> results, int ok, int failed, int skipped) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Result(String kind, long id, Outcome outcome, String errorCode, String previousErrorCode) {
    }

    public enum Outcome {
        OK, FAILED, SKIPPED
    }
}
