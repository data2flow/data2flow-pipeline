package net.java21.data2flow.pipeline.ingest.domain;

/** 실패 단계({@code dlq_items.stage}) */
public enum DlqStage {
    DECODE, SCRIPT, STORE, PUBLISH
}
