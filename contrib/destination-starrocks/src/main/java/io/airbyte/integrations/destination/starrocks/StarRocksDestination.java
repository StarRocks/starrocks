/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.starrocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.functional.CheckedBiFunction;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.AirbyteMessageConsumer;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.StandardNameTransformer;
import io.airbyte.integrations.destination.record_buffer.FileBuffer;
import io.airbyte.integrations.destination.record_buffer.SerializableBuffer;
import io.airbyte.protocol.models.v0.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class StarRocksDestination extends BaseConnector implements Destination {

  private static final Logger LOGGER = LoggerFactory.getLogger(StarRocksDestination.class);
  private static final StandardNameTransformer namingResolver = new StandardNameTransformer();

  private static Connection conn = null;

  public static void main(String[] args) throws Exception {
    new IntegrationRunner(new StarRocksDestination()).run(args);
  }

  @Override
  public AirbyteConnectionStatus check(JsonNode config) {
    try {
      Preconditions.checkNotNull(config);
      conn = SqlUtil.createJDBCConnection(config);
    } catch (final Exception e) {
      return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.FAILED).withMessage(e.getMessage());
    }
    return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
  }

  @Override
  public AirbyteMessageConsumer getConsumer(JsonNode config,
                                            ConfiguredAirbyteCatalog configuredCatalog,
                                            Consumer<AirbyteMessage> outputRecordCollector) {

    LOGGER.info("JsonNode config: \n"+config.toPrettyString());
    try {
      if (conn == null){
        conn = SqlUtil.createJDBCConnection(config);
      }
    } catch (SQLException | ClassNotFoundException e) {
      e.printStackTrace();
    }

    return StarRocksBufferedConsumerFactory.create(outputRecordCollector, conn, namingResolver, config, configuredCatalog);
  }

}
