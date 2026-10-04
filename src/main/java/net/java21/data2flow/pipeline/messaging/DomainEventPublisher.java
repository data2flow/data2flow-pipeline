package net.java21.data2flow.pipeline.messaging;

import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.EventPayload;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.pipeline.common.TransientFailures.PublishFailedException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 도메인 이벤트 발행(topic {@code data2flow.events}, 라우팅 키 = {@code type}, 봉투 {@link DomainEvent}). pipeline이 내는 것:
 * EVT-DEV-02 {@code device.connectivity.changed}, EVT-ING-05 {@code ingest.alert.*}, EVT-ING-06 {@code ingest.gap.detected},
 * EVT-TSD-03 {@code aggregates.recomputed}, EVT-TSD-06 {@code partition.warning}, EVT-ACT-07 {@code device.state.reported}(LoRaWAN 업링크
 * 신호, {@link net.java21.data2flow.pipeline.ingest.domain.LoRaWanUplinkSignal}). 발행 확인을 기다리고 실패하면 예외(호출하는 쪽이
 * 다시 시도, 하위는 messageId로 중복을 거른다).
 */
public class DomainEventPublisher {

    private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(10);

    private final RabbitTemplate rabbit;
    private final Clock clock;
    private final MessageCodec codec = MessageCodec.create();

    public DomainEventPublisher(RabbitTemplate rabbit, Clock clock) {
        this.rabbit = rabbit;
        this.clock = clock;
    }

    public <P extends EventPayload> DomainEvent<P> publish(EventType type, long organizationId, P payload) {
        return send(DomainEvent.of(type, organizationId, payload, null, clock), type);
    }

    /**
     * 정해진 messageId로 발행한다. 같은 원본을 다시 처리해 다시 낼 때 같은 ID가 되게 해 하위가 중복을 거르게 한다
     * (예: EVT-ACT-07 LoRaWAN 업링크 신호, reliability-and-ha.md §2 ⑤).
     */
    public <P extends EventPayload> DomainEvent<P> publish(EventType type, long organizationId, P payload, java.util.UUID messageId) {
        DomainEvent<P> generated = DomainEvent.of(type, organizationId, payload, null, clock);
        return send(new DomainEvent<>(generated.v(), messageId, generated.type(), organizationId, generated.occurredAt(), null, payload), type);
    }

    private <P extends EventPayload> DomainEvent<P> send(DomainEvent<P> event, EventType type) {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        props.setMessageId(event.messageId().toString());
        MessageHeaders.of(event).forEach(props::setHeader);
        CorrelationData correlation = new CorrelationData(event.messageId().toString());
        rabbit.send(MessagingNames.EXCHANGE_EVENTS, event.type(), new Message(codec.write(event), props), correlation);
        try {
            CorrelationData.Confirm confirm = correlation.getFuture().get(CONFIRM_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.ack()) {
                throw new PublishFailedException("이벤트 발행이 거부되었습니다: " + type.routingKey() + " " + confirm.reason(), null);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("이벤트 발행 대기가 중단되었습니다", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new PublishFailedException("이벤트 발행 확인 실패: " + type.routingKey(), e);
        }
        return event;
    }
}
