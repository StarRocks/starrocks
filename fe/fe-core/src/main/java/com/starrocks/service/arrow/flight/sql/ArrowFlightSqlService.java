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

import com.starrocks.service.FrontendOptions;
import com.starrocks.service.arrow.flight.sql.auth.ArrowFlightSqlAuthenticator;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlTokenManager;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class ArrowFlightSqlService {

    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlService.class);

    private final Location location;
    private final FlightServer flightServer;

    protected volatile boolean running = false;

    public ArrowFlightSqlService(int port) {
        // Disable Arrow Flight SQL feature if port is not set to a positive value.
        if (port <= 0) {
            this.location = null;
            this.flightServer = null;
            return;
        }

        BufferAllocator allocator = new RootAllocator();
        this.location = Location.forGrpcInsecure("0.0.0.0", port);

        ArrowFlightSqlTokenManager arrowFlightSqlTokenManager = new ArrowFlightSqlTokenManager();
        ArrowFlightSqlSessionManager arrowFlightSqlSessionManager =
                new ArrowFlightSqlSessionManager(arrowFlightSqlTokenManager);

        ArrowFlightSqlServiceImpl producer =
                new ArrowFlightSqlServiceImpl(arrowFlightSqlSessionManager,
                        Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port));
        ArrowFlightSqlAuthenticator arrowFlightSqlAuthenticator =
                new ArrowFlightSqlAuthenticator(arrowFlightSqlTokenManager);

        this.flightServer = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(arrowFlightSqlAuthenticator)
                .build();
    }

    public void start() {
        if (location == null) {
            LOG.info("[ARROW] Arrow Flight SQL server is disabled. You can modify `arrow_flight_port` in `fe.conf` " +
                    "to a positive value to enable it.");
            return;
        }

        if (running) {
            return;
        }

        try {
            flightServer.start();
            running = true;
            LOG.info("[ARROW] Arrow Flight SQL server starts on {}:{}.",
                    location.getUri().getHost(), location.getUri().getPort());
        } catch (Exception e) {
            LOG.error("[ARROW] Failed to start Arrow Flight SQL server on {}:{}. Its port might be occupied. You can " +
                            "modify `arrow_flight_port` in `fe.conf` to an unused port or set it to -1 to disable it.",
                    location.getUri().getHost(),
                    location.getUri().getPort(), e);
            System.exit(-1);
        }
    }

    public void stop() {
        if (!running) {
            return;
        }

        running = false;
        try {
            LOG.info("[ARROW] Stopping Arrow Flight SQL server .");
            flightServer.shutdown();
            flightServer.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("[ARROW] Interrupted while stopping Arrow Flight SQL server", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("[ARROW] Error while stopping Arrow Flight SQL server", e);
        }
    }

}