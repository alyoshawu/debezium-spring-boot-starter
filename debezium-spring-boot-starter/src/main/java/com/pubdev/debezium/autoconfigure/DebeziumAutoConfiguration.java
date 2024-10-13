package com.pubdev.debezium.autoconfigure;

import com.pubdev.debezium.listener.DebeziumListener;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * description: Debezium自动配置类.
 *
 * @author wyl
 * @version V1.0
 * @date 2024/10/12 08:52
 * @description <p>Debezium自动配置类</p>
 **/
@EnableConfigurationProperties(DebeziumProperties.class)
public class DebeziumAutoConfiguration {
    @Bean(destroyMethod = "destroy", initMethod = "init")
    public DebeziumListener debeziumListener(DebeziumProperties debeziumProperties) {
        return new DebeziumListener(debeziumProperties);
    }
}
