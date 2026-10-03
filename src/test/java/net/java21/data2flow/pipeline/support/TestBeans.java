package net.java21.data2flow.pipeline.support;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** 통합 테스트 빈: 바꿀 수 있는 시계(@Primary) */
@TestConfiguration(proxyBeanMethods = false)
public class TestBeans {

    @Bean
    @Primary
    MutableClock testClock() {
        return new MutableClock(MutableClock.T0);
    }
}
