package net.java21.data2flow.pipeline.messaging;

import org.springframework.amqp.rabbit.listener.AsyncConsumerStartedEvent;
import org.springframework.context.event.EventListener;

/** 설정 변경 큐 소비자가 (다시) 시작되면 놓친 메시지가 있을 수 있으므로 캐시를 모두 지운다(architecture.md §4.1 "원천은 DB") */
public class ConfigReconnectHandler {

    private final ConfigChangeListener listener;

    public ConfigReconnectHandler(ConfigChangeListener listener) {
        this.listener = listener;
    }

    @EventListener
    public void onConsumerStarted(AsyncConsumerStartedEvent event) {
        listener.invalidateAll();
    }
}
