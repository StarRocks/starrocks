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

package com.starrocks.qe;

import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.HttpConnectProcessor;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectContext;
import com.starrocks.service.arrow.flight.sql.ArrowFlightSqlConnectProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An INSERT that reads Hive turns the connector metadata cache off for itself, on its connection's context
 * (InsertAnalyzer). The MySQL protocol resets that for every command; the HTTP and Arrow Flight SQL contexts outlive a
 * statement too -- the keep-alive connection, the session token -- so every later Hive query on them would skip the
 * cache unless they reset it for each statement as well.
 */
public class ConnectorMetadataCacheResetTest {

    @Test
    public void testHttpStatementStartsWithTheCacheSetting() throws Exception {
        HttpConnectContext ctx = new HttpConnectContext();
        ctx.setUseConnectorMetadataCache(Optional.of(false));
        AtomicReference<Optional<Boolean>> seen = new AtomicReference<>();
        new HttpConnectProcessor(ctx) {
            @Override
            protected void handleQuery() {
                seen.set(ctx.getUseConnectorMetadataCache());
            }
        }.processOnce();
        Assertions.assertEquals(Optional.empty(), seen.get());
    }

    @Test
    public void testArrowFlightSqlStatementStartsWithTheCacheSetting() {
        ArrowFlightSqlConnectContext ctx = new ArrowFlightSqlConnectContext("token");
        ctx.setUseConnectorMetadataCache(Optional.of(false));
        AtomicReference<Optional<Boolean>> seen = new AtomicReference<>();
        new ArrowFlightSqlConnectProcessor(ctx, "SELECT 1") {
            @Override
            protected void handleQuery() {
                seen.set(ctx.getUseConnectorMetadataCache());
            }
        }.processOnce();
        Assertions.assertEquals(Optional.empty(), seen.get());
    }
}
