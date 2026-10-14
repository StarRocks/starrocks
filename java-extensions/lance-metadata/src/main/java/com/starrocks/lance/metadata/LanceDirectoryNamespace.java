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

package com.starrocks.lance.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.namespace.DirectoryNamespace;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** FE-only SDK adapter. Only strings cross the isolated class loader boundary. */
public final class LanceDirectoryNamespace {
    private static final ObjectMapper JSON = new ObjectMapper();

    private LanceDirectoryNamespace() {
    }

    public static String listTables(String warehouse, String options) throws Exception {
        try (DirectoryNamespace namespace = open(warehouse, options)) {
            Set<String> names = new TreeSet<>();
            Set<String> tokens = new HashSet<>();
            String token = null;
            do {
                ListTablesResponse response = namespace.listTables(
                        new ListTablesRequest().id(List.of()).pageToken(token).limit(1000));
                names.addAll(response.getTables());
                token = response.getPageToken();
                if (token != null && !token.isEmpty() && !tokens.add(token)) {
                    throw new IllegalStateException("Lance directory listing repeated a page token");
                }
            } while (token != null && !token.isEmpty());
            return JSON.writeValueAsString(names);
        }
    }

    public static String describeTable(String warehouse, String options, String table) throws Exception {
        try (DirectoryNamespace namespace = open(warehouse, options)) {
            DescribeTableResponse response = namespace.describeTable(
                    new DescribeTableRequest().id(List.of(table)));
            if (response.getLocation() == null) {
                throw new IllegalStateException("Lance directory returned incomplete table metadata");
            }
            // Do not return credentials or SDK-specific objects to the FE class loader.
            Map<String, String> storage = JSON.readValue(options, new TypeReference<Map<String, String>>() {});
            try (Dataset dataset = Dataset.open().uri(response.getLocation())
                    .readOptions(new ReadOptions.Builder().setStorageOptions(storage).build()).build()) {
                // Namespace JSON types omit details such as decimal precision. Read the persisted schema instead.
                // getLanceSchema does not allocate Arrow C Data buffers in the FE.
                return JSON.writeValueAsString(Map.of("location", response.getLocation(), "schema",
                        JSON.readTree(dataset.getLanceSchema().asArrowSchema().toJson())));
            }
        }
    }

    private static DirectoryNamespace open(String warehouse, String options) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("root", warehouse);
        // Discover existing datasets without creating or updating a namespace manifest.
        properties.put("manifest_enabled", "false");
        properties.put("dir_listing_enabled", "true");
        Map<String, String> storage = JSON.readValue(options, new TypeReference<Map<String, String>>() {});
        storage.forEach((key, value) -> properties.put("storage." + key, value));
        DirectoryNamespace namespace = new DirectoryNamespace();
        try {
            // Listing and describing schemas use JSON, not Arrow data buffers.
            namespace.initialize(properties, null);
            return namespace;
        } catch (Exception | LinkageError e) {
            namespace.close();
            throw e;
        }
    }
}
