package net.java21.data2flow.pipeline.common;

import net.java21.data2flow.pipeline.device.service.CoreUnavailableException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;

import java.sql.SQLTransientException;

/**
 * 다시 시도하면 될 일시 장애인지(DB 연결·시간 초과, core-api 장애, 스트림 발행 확인 실패). 이런 장애에서는 오프셋을 넘기지 않고
 * 같은 메시지를 다시 처리한다(유실 0, NFR-02.02).
 */
public final class TransientFailures {

    private TransientFailures() {
    }

    public static boolean isTransient(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof CoreUnavailableException || t instanceof PublishFailedException
                    || t instanceof DataAccessResourceFailureException || t instanceof CannotGetJdbcConnectionException
                    || t instanceof TransientDataAccessException || t instanceof QueryTimeoutException
                    || t instanceof CannotCreateTransactionException || t instanceof TransactionSystemException
                    || t instanceof SQLTransientException || t instanceof java.net.SocketException
                    || t instanceof java.io.EOFException) {
                return true;
            }
            if (t instanceof java.sql.SQLException sql && sql.getSQLState() != null
                    && (sql.getSQLState().startsWith("08") || sql.getSQLState().startsWith("57P")
                    || "53300".equals(sql.getSQLState()))) {
                return true; // 연결 오류(08xxx), 관리자 종료(57P01), 연결 수 초과
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /** 스트림·이벤트 발행 확인(confirm)을 받지 못했다 */
    public static class PublishFailedException extends RuntimeException {
        public PublishFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
