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

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.arrow.flight.FlightServer;
import org.junit.Test;

import java.io.IOException;
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
                times = 0;
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
}
