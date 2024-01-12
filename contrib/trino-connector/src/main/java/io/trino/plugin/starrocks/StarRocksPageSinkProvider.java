// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/StarRocksFE.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package io.trino.plugin.starrocks;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class StarRocksPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StreamLoadProperties streamLoadProperties;
    private final HttpClientBuilder clientBuilder;

    @Inject
    public StarRocksPageSinkProvider(StreamLoadProperties streamLoadProperties)
    {
        this.streamLoadProperties = requireNonNull(streamLoadProperties, "streamLoadProperties is null");
        this.clientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy()
                {
                    @Override
                    protected boolean isRedirectable(String method)
                    {
                        return true;
                    }
                });
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        StarRocksOutputTableHandle starRocksOutputTableHandle = (StarRocksOutputTableHandle) tableHandle;
        StarRocksOperationApplier applier = new StarRocksOperationApplier(
                starRocksOutputTableHandle.getSchemaName(), starRocksOutputTableHandle.getTableName(), starRocksOutputTableHandle.getTemporaryTableName(), starRocksOutputTableHandle.getColumnNames(), starRocksOutputTableHandle.getIsPkTable(), streamLoadProperties, clientBuilder);
        return new StarRocksPageSink(starRocksOutputTableHandle, applier, pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        StarRocksOutputTableHandle starRocksOutputTableHandle = (StarRocksOutputTableHandle) tableHandle;
        StarRocksOperationApplier applier = new StarRocksOperationApplier(
                starRocksOutputTableHandle.getSchemaName(), starRocksOutputTableHandle.getTableName(), starRocksOutputTableHandle.getTemporaryTableName(), starRocksOutputTableHandle.getColumnNames(), starRocksOutputTableHandle.getIsPkTable(), streamLoadProperties, clientBuilder);
        return new StarRocksPageSink(starRocksOutputTableHandle, applier, pageSinkId);
    }
}
