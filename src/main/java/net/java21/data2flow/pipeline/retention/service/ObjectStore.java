package net.java21.data2flow.pipeline.retention.service;

/** 콜드 보관 오브젝트 저장소(TSD-05.02). 운영 구현은 {@link S3ObjectStore} */
public interface ObjectStore {

    void put(String key, byte[] body, String contentType);

    byte[] get(String key);

    /** 저장소 오류(일시 장애 포함). 콜드 보관은 다음 야간 작업에 다시 한다(원본은 지우지 않음) */
    class ObjectStoreException extends RuntimeException {
        public ObjectStoreException(String message) {
            super(message);
        }
    }
}
