package net.java21.data2flow.pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** data2flow-pipeline: 디코딩, DECODE·TRANSFORM 스크립트(GraalJS), 기기 식별·자동 등록, 검증, 저장, 집계·파티션 관리 */
@SpringBootApplication
public class PipelineApplication {

    public static void main(String[] args) {
        SpringApplication.run(PipelineApplication.class, args);
    }
}
