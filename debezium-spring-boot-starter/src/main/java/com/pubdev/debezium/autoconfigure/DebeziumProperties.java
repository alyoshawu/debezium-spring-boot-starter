package com.pubdev.debezium.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * description: Debezium配置.
 *
 * @author wyl
 * @version V1.0
 * @date 2024/10/12 08:48
 * @description <p>Debezium配置</p>
 **/
@Data
@ConfigurationProperties("debezium")
public class DebeziumProperties {
    private String connectorClass = "io.debezium.connector.postgresql.PostgresConnector";
    private String offsetStorage = "org.apache.kafka.connect.storage.MemoryOffsetBackingStore";
    private String offsetStorageFileFilename;
    private Integer offsetFlushIntervalMs = 10000;
    private String name;
    private String databaseServerName;
    private String databaseHostname;
    private Integer databasePort = 5432;
    private String databaseUser;
    private String databasePassword;
    private String databaseDbname;
    private String topicPrefix;
    private String tableWhitelist;
}
