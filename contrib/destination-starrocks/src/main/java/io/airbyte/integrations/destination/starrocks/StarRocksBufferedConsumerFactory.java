/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.functional.CheckedFunction;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.destination.NamingConventionTransformer;
import io.airbyte.integrations.destination.buffered_stream_consumer.BufferedStreamConsumer;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnCloseFunction;
import io.airbyte.integrations.destination.buffered_stream_consumer.OnStartFunction;
import io.airbyte.integrations.destination.buffered_stream_consumer.RecordWriter;
import io.airbyte.integrations.destination.record_buffer.InMemoryRecordBufferingStrategy;
import io.airbyte.integrations.destination.starrocks.stream.StreamLoadUtils;
import io.airbyte.protocol.models.v0.*;
import org.apache.http.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StarRocksBufferedConsumerFactory {
  private static final int MAX_BATCH_SIZE_BYTES = 128 * 1024 * 1024; // 512MB

  private static final Logger LOGGER = LoggerFactory.getLogger(StarRocksBufferedConsumerFactory.class);

  public static AirbyteMessageConsumer create(final Consumer<AirbyteMessage> outputRecordCollector,
                                              final Connection conn,
                                              final NamingConventionTransformer namingResolver,
                                              final JsonNode config,
                                              final ConfiguredAirbyteCatalog catalog) {
    final List<StarRocksWriteConfig> writeConfigs = catalog.getStreams()
            .stream().map(createWriteConfig(namingResolver, config)).collect(Collectors.toList());

    return new BufferedStreamConsumer(
        outputRecordCollector,
        onStartFunction(conn, writeConfigs),
        new InMemoryRecordBufferingStrategy(recordWriterFunction(writeConfigs), MAX_BATCH_SIZE_BYTES),
        onCloseFunction(conn, writeConfigs),
        catalog,
        isValidFunction());
  }

  private static OnStartFunction onStartFunction(final Connection conn,
                                                 final List<StarRocksWriteConfig> writeConfigs) {
    return () -> {
      LOGGER.info("Preparing tmp tables in destination started for {} streams", writeConfigs.size());
      for (final StarRocksWriteConfig writeConfig : writeConfigs) {

        SqlUtil.createDatabaseIfNotExist(conn, writeConfig.getDatabase());

        final String tmpTableName = writeConfig.getTmpTableName();
        LOGGER.info("Preparing tmp table in destination started for stream {}. tmp table name: {}", writeConfig.getStreamName(),
            tmpTableName);

        SqlUtil.createTableIfNotExist(conn, tmpTableName);
        SqlUtil.truncateTable(conn, tmpTableName);
      }
      LOGGER.info("Preparing tmp tables in destination completed.");
    };
  }

  private static RecordWriter<AirbyteRecordMessage> recordWriterFunction(final List<StarRocksWriteConfig> writeConfigs) {
    final Map<AirbyteStreamNameNamespacePair, StarRocksWriteConfig> pairToWriteConfig = writeConfigs.stream()
        .collect(Collectors.toUnmodifiableMap(writeConfig ->
          new AirbyteStreamNameNamespacePair(writeConfig.getStreamName(), writeConfig.getNamespace())
        , Function.identity()));

    return (pair, records) -> {
      if (!pairToWriteConfig.containsKey(pair)) {
        throw new IllegalArgumentException(
            String.format("Message contained record from a stream that was not in the catalog: %s", pair));
      }

      final StarRocksWriteConfig writeConfig = pairToWriteConfig.get(pair);
      final StreamLoader streamLoader = writeConfig.getStreamLoader();
      streamLoader.send(records);
    };
  }

  private static OnCloseFunction onCloseFunction(final Connection conn,
                                                 final List<StarRocksWriteConfig> writeConfigs) {
    return (hasFailed) -> {
      // copy data
      if (!hasFailed) {
        for (final StarRocksWriteConfig writeConfig : writeConfigs) {
          final String srcTableName = writeConfig.getTmpTableName();
          final String dstTableName = writeConfig.getOutputTableName();
          LOGGER.info("Finalizing stream {}. tmp table {}, final table {}", writeConfig.getStreamName(), srcTableName,
              dstTableName);

          switch (writeConfig.getSyncMode()) {
            case OVERWRITE -> {
              SqlUtil.dropTableIfExists(conn, dstTableName);
              SqlUtil.renameTable(conn, srcTableName, dstTableName);
            }
            case APPEND, APPEND_DEDUP -> {
              SqlUtil.createTableIfNotExist(conn, dstTableName);
              SqlUtil.insertFromTable(conn, srcTableName, dstTableName);
            }
            default -> throw new IllegalStateException("Unrecognized sync mode: " + writeConfig.getSyncMode());
          }
        }
        LOGGER.info("Finalizing tables in destination completed.");
      }

      // clean up
      LOGGER.info("Cleaning tmp tables in destination started for {} streams", writeConfigs.size());
      for (final StarRocksWriteConfig writeConfig : writeConfigs) {
        final String tmpTableName = writeConfig.getTmpTableName();
        LOGGER.info("Cleaning tmp table in destination started for stream {}.tmp table name: {}", writeConfig.getStreamName(),
            tmpTableName);

        SqlUtil.dropTableIfExists(conn, tmpTableName);
      }
      LOGGER.info("Cleaning tmp tables in destination completed.");
    }

    ;
  }

  private static CheckedFunction<JsonNode, Boolean, Exception> isValidFunction() {
    return jsonNode -> true;
  }


  private static Function<ConfiguredAirbyteStream, StarRocksWriteConfig> createWriteConfig(NamingConventionTransformer namingResolver, JsonNode config){
    return stream -> {
      String feHost = config.get(StarRocksConstants.KEY_FE_HOST).asText();
      String user = config.get(StarRocksConstants.KEY_USER)==null ?
              StarRocksConstants.DEFAULT_USER :
              config.get(StarRocksConstants.KEY_USER).asText();
      String password = config.get(StarRocksConstants.KEY_PWD)==null ?
              StarRocksConstants.DEFAULT_PWD :
              config.get(StarRocksConstants.KEY_PWD).asText();
      int httpPort = config.get(StarRocksConstants.KEY_FE_HTTP_PORT).asInt(StarRocksConstants.DEFAULT_FE_HTTP_PORT);

      final DestinationSyncMode syncMode = stream.getDestinationSyncMode();
      Preconditions.checkNotNull(syncMode, "Undefined destination sync mode");

      AirbyteStream airbyteStream = stream.getStream();

      String database = config.has(StarRocksConstants.KEY_DB)
              ? config.get(StarRocksConstants.KEY_DB).asText()
              : airbyteStream.getNamespace();
      database = database==null
              ? namingResolver.getNamespace(StarRocksConstants.DEFAULT_DB)
              : namingResolver.getNamespace(database);
//        SqlUtil.createDatabaseIfNotExist(conn, database);

      final String streamName = airbyteStream.getName();
      final String tableName = namingResolver.getIdentifier(streamName);
      final String tmpTableName = namingResolver.getTmpTableName(streamName);


      StreamLoadProperties streamLoadProperties = new StreamLoadProperties(database,tmpTableName, feHost.split(","), httpPort, user, password);
      StreamLoader streamLoader = new DefaultStreamLoader(streamLoadProperties);

    return StarRocksWriteConfig.builder()
            .streamLoadProperties(streamLoadProperties)
            .outputTableName(tableName)
            .database(database)
            .tmpTableName(tmpTableName)
            .syncMode(syncMode)
            .streamLoader(streamLoader)
            .namespace(airbyteStream.getNamespace())
            .streamName(streamName)
            .build();
    };
  }
}
