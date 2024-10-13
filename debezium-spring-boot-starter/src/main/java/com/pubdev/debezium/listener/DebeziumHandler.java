package com.pubdev.debezium.listener;

import io.debezium.data.Envelope;

/**
 * description: Debezium的处理器接口.
 *
 * @author wyl
 * @version V1.0
 * @date 2024/10/12 09:43
 * @description <p>Debezium的处理器接口</p>
 **/
@FunctionalInterface
public interface DebeziumHandler<T> {
    void handle(Envelope.Operation operation, T message);
}
