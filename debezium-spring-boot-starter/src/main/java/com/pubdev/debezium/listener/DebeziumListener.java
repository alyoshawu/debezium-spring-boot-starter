package com.pubdev.debezium.listener;

import com.alibaba.fastjson2.JSON;
import com.pubdev.debezium.autoconfigure.DebeziumProperties;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.GenericTypeResolver;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DebeziumListener implements ApplicationContextAware {
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private Map<DebeziumHandler, Class<?>> insClazzMap;

    public DebeziumListener(DebeziumProperties debeziumProperties) {
        Properties properties = Configuration.create()
                .with("connector.class", debeziumProperties.getConnectorClass())
                .with("offset.storage", debeziumProperties.getOffsetStorage())
                .with("offset.storage.file.filename", debeziumProperties.getOffsetStorageFileFilename())
                .with("offset.flush.interval.ms", debeziumProperties.getOffsetFlushIntervalMs())
                .with("name", debeziumProperties.getName())
                .with("database.server.name", debeziumProperties.getDatabaseServerName())
                .with("database.hostname", debeziumProperties.getDatabaseHostname())
                .with("database.port", debeziumProperties.getDatabasePort())
                .with("database.user", debeziumProperties.getDatabaseUser())
                .with("database.password", debeziumProperties.getDatabasePassword())
                .with("database.dbname", debeziumProperties.getDatabaseDbname())
                .with("topic.prefix", debeziumProperties.getTopicPrefix())
                .with("table.whitelist", debeziumProperties.getTableWhitelist())
                .build().asProperties();
        engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(properties)
                .notifying(this::handleChangeEvent)
                .build();
    }

    private void init() {
        executor.execute(engine);
    }

    private void destroy() throws IOException {
        if (engine != null) {
            engine.close();
        }

        executor.shutdown();
    }

    @SuppressWarnings("t")
    @SneakyThrows
    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {
        SourceRecord sourceRecord = sourceRecordRecordChangeEvent.record();
        Struct sourceRecordValue = (Struct) sourceRecord.value();

        if (sourceRecordValue != null) {
            String oprStr = (String) sourceRecordValue.get(Envelope.FieldName.OPERATION);
            Envelope.Operation operation = Envelope.Operation.forCode(oprStr);

            if ("crud".contains(operation.code())) {
                String record = operation == Envelope.Operation.DELETE ? Envelope.FieldName.BEFORE : Envelope.FieldName.AFTER;
                Struct struct = (Struct) sourceRecordValue.get(record);

                List<Field> fields = struct.schema().fields();
                Map<String, Object> message = new HashMap<>(fields.size());
                fields.stream()
                        .map(Field::name)
                        .forEach(v -> message.put(v, struct.get(v)));

                if (message.get("instance_name") != null) {
                    return;
                }

                Object className = message.get("__class__");
                Class<?> clazz = className == null ? null : Class.forName((String) className);
                for (Map.Entry<DebeziumHandler, Class<?>> i : insClazzMap.entrySet()) {
                    if (i.getValue() == clazz) {
                        i.getKey().handle(operation, JSON.to(clazz, message));
                    } else if (clazz == null && Map.class == i.getValue()) {
                        i.getKey().handle(operation, message);
                    }
                }
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Collection<DebeziumHandler> values = applicationContext.getBeansOfType(DebeziumHandler.class).values();
        insClazzMap = new HashMap<>(values.size());
        for (DebeziumHandler<?> i : values) {
            insClazzMap.put(i, GenericTypeResolver.resolveTypeArgument(i.getClass(), DebeziumHandler.class));
        }
    }
}
