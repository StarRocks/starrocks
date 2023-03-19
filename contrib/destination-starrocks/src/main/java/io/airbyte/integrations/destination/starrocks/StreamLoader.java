package io.airbyte.integrations.destination.starrocks;



import io.airbyte.protocol.models.v0.AirbyteRecordMessage;

import java.util.List;

public interface StreamLoader {

    void close();

    StreamLoadResponse send(List<AirbyteRecordMessage> records) throws Exception;

}
