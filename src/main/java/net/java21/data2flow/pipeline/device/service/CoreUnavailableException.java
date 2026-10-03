package net.java21.data2flow.pipeline.device.service;

/** core-api 일시 장애(연결 실패·시간 초과·5xx). 처리는 오프셋을 넘기지 않고 다시 시도한다 */
public class CoreUnavailableException extends RuntimeException {

    public CoreUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
