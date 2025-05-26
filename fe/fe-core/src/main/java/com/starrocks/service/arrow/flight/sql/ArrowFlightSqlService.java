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
import com.starrocks.service.arrow.flight.sql.auth2.ArrowFlightSqlAuthenticator;
import com.starrocks.service.arrow.flight.sql.session.ArrowFlightSqlSessionManager;
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

    // Arrow Flight server encapsulation class, based on gRPC implementation
    private final FlightServer flightServer;

    private final Location feEndpoint;

    private final ArrowFlightSqlServiceImpl producer;

    protected volatile boolean running;

    public ArrowFlightSqlService(int port) {
        // Disable Arrow Flight SQL feature if port is not set to a positive value.
        if (port <= 0) {
            this.location = null;
            this.flightServer = null;
            this.feEndpoint = null;
            this.producer = null;
            return;
        }

        // Memory allocator: A memory allocator is needed to manage memory.
        BufferAllocator allocator = new RootAllocator();
        this.location = Location.forGrpcInsecure("0.0.0.0", port);
        this.feEndpoint = Location.forGrpcInsecure(FrontendOptions.getLocalHostAddress(), port);

        ArrowFlightSqlSessionManager sessionManager = new ArrowFlightSqlSessionManager();
        ArrowFlightSqlAuthenticator authenticator = new ArrowFlightSqlAuthenticator(sessionManager);

        // Request handler: Processing client SQL requests.
        this.producer = new ArrowFlightSqlServiceImpl(sessionManager, feEndpoint);

        // Constructs the server object of the Arrow Flight SQL.
        this.flightServer = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(authenticator)
                .build();
    }

    public void start() {
        if (running) {
            return;
        }

        if (location == null) {
            LOG.info("[ARROW] Arrow Flight SQL server is disabled. You can modify `arrow_flight_port` in `fe.conf` " +
                    "to a positive value to enable it.");
            return;
        }

        try {
            flightServer.start();
            running = true;
            LOG.info("[ARROW] Arrow Flight SQL server start [location={}] [feEndpoint={}].", location, feEndpoint);
            flightServer.awaitTermination();
        } catch (InterruptedException e) {
            LOG.warn("[ARROW] Interrupted while stopping Arrow Flight SQL server", e);
            Thread.currentThread().interrupt();
            System.exit(-1);
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
            producer.close();
        } catch (InterruptedException e) {
            LOG.warn("[ARROW] Interrupted while stopping Arrow Flight SQL server", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.warn("[ARROW] Error while stopping Arrow Flight SQL server", e);
        }
    }

}