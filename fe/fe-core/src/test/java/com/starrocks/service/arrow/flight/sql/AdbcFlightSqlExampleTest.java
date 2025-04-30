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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AdbcFlightSqlExampleTest {
    @Test
    public void testAdbcFlightSqlExample() throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            FlightSqlDriver driver = new FlightSqlDriver(allocator);

            Map<String, Object> parameters = new HashMap<>();
            String host = "localhost";
            int port = 9408;
            String uri = Location.forGrpcInsecure(host, port).getUri().toString();

            AdbcDriver.PARAM_URI.set(parameters, uri);
            AdbcDriver.PARAM_USERNAME.set(parameters, "root");
            AdbcDriver.PARAM_PASSWORD.set(parameters, "");

            try (AdbcDatabase database = driver.open(parameters);
                    AdbcConnection connection = database.connect();
                    AdbcStatement statement = connection.createStatement()) {

                statement.setSqlQuery("SELECT * FROM INFORMATION_SCHEMA.tables;");

                try (AdbcStatement.QueryResult result = statement.executeQuery();
                        ArrowReader reader = result.getReader()) {

                    int batchCount = 0;
                    while (reader.loadNextBatch()) {
                        batchCount++;
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        System.out.println("Batch " + batchCount + ":");
                        System.out.println(root.contentToTSVString());
                    }

                    System.out.println("Total batches: " + batchCount);
                }
            }
        }
    }
}