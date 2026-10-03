package net.java21.data2flow.pipeline.messaging;

import com.rabbitmq.stream.Message;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.pipeline.ingest.service.RawStreamConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * 읽을 수 없는 {@code data2flow.raw} 메시지(형식 오류·모르는 스키마 버전)를 {@code data2flow.dlx} → {@code pipeline.raw.dlq}로
 * 보낸다(reliability-and-ha.md §2 "공통", contracts README §13 "재시도하지 말고 DLQ로"). 조직을 알 수 없어 raw_messages에는 넣지 않는다.
 */
public class UnreadableRawSink implements RawStreamConsumer.UnreadableSink {

    public static final String QUEUE = "pipeline.raw";

    private static final Logger log = LoggerFactory.getLogger(UnreadableRawSink.class);

    private final RabbitTemplate rabbit;

    public UnreadableRawSink(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    @Override
    public void send(Message message, String stream, long offset, Exception reason) {
        MessageProperties props = new MessageProperties();
        props.setHeader("x-stream", stream);
        props.setHeader("x-stream-offset", offset);
        props.setHeader("x-reason", String.valueOf(reason.getMessage()));
        try {
            rabbit.send(MessagingNames.EXCHANGE_DLX, QUEUE,
                    new org.springframework.amqp.core.Message(message.getBodyAsBinary(), props));
            log.warn("읽을 수 없는 원본을 {}로 보냈습니다({}@{}): {}", MessagingNames.deadLetterQueue(QUEUE), stream, offset,
                    reason.getMessage());
        } catch (RuntimeException e) {
            log.error("읽을 수 없는 원본을 DLQ로 보내지 못했습니다({}@{}): {}", stream, offset, e.getMessage());
        }
    }
}
