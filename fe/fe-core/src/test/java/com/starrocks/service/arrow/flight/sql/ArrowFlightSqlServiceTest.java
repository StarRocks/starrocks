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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.common.Config;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.arrow.flight.FlightServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlServiceTest {
    @Test
    public void testDisable() {
        ArrowFlightSqlService service = new ArrowFlightSqlService(-1);
        service.start();
        service.stop();
    }

    /**
     * Mock {@link FlightServer.Builder#build()}.
     */
    @Test
    public void testEnable(@Mocked FlightServer server) throws IOException, InterruptedException {
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer build() {
                return server;
            }
        };

        new Expectations() {
            {
                server.start();
                result = server;
                times = 1;
            }

            {
                server.shutdown();
                times = 1;
            }

            {
                server.awaitTermination();
                times = 1;
            }

            {
                server.awaitTermination(anyLong, TimeUnit.SECONDS);
                result = true;
                times = 1;
            }
        };

        ArrowFlightSqlService service = new ArrowFlightSqlService(1234);
        service.start();
        service.stop();
    }

    /**
     * The header limit is what decides whether a JWT listing many groups gets through at all: gRPC's
     * own default is 8 KB and applies before any authenticator runs. Asserts the configured value is
     * what reaches the server builder, both the default and an override.
     */
    @Test
    public void testHeaderLimitComesFromTheConfig() {
        List<Integer> passed = new ArrayList<>();
        new MockUp<FlightServer.Builder>() {
            @Mock
            public FlightServer.Builder setMaxHeaderListSize(Invocation invocation, int size) {
                passed.add(size);
                return invocation.getInvokedInstance();
            }

            // A real builder, so the call above is intercepted on the instance the service
            // actually configures; nothing is started, so no server object is needed.
            @Mock
            public FlightServer build() {
                return null;
            }
        };

        int saved = Config.arrow_flight_max_header_size;
        try {
            new ArrowFlightSqlService(1234);
            Config.arrow_flight_max_header_size = 65536;
            new ArrowFlightSqlService(1234);
        } finally {
            Config.arrow_flight_max_header_size = saved;
        }
        Assertions.assertEquals(List.of(32768, 65536), passed);
    }
}
