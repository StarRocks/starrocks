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

package com.starrocks.connector.hive;

import com.google.common.base.Strings;
import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class AvroSchemaResolver {
    public static final String AVRO_SCHEMA_LITERAL = "avro.schema.literal";
    public static final String AVRO_SCHEMA_URL = "avro.schema.url";

    private final Configuration hadoopConf;

    public AvroSchemaResolver(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public Optional<String> resolve(HiveTable table) {
        if (table.getStorageFormat() != HiveStorageFormat.AVRO) {
            return Optional.empty();
        }

        Map<String, String> serdeProperties =
                table.getSerdeProperties() == null ? Collections.emptyMap() : table.getSerdeProperties();
        Map<String, String> tableProperties =
                table.getProperties() == null ? Collections.emptyMap() : table.getProperties();
        String literal = getConfiguredSchemaValue(serdeProperties, tableProperties, AVRO_SCHEMA_LITERAL);
        if (isConfigured(literal)) {
            return Optional.of(parseSchema(
                    literal, String.format("%s.%s", table.getCatalogDBName(), table.getCatalogTableName())));
        }

        String schemaUrl = getConfiguredSchemaValue(serdeProperties, tableProperties, AVRO_SCHEMA_URL);
        if (!isConfigured(schemaUrl)) {
            return Optional.empty();
        }
        return Optional.of(parseSchema(readSchemaUrl(schemaUrl), schemaUrl));
    }

    private String getConfiguredSchemaValue(
            Map<String, String> serdeProperties, Map<String, String> tableProperties, String key) {
        String serdeValue = serdeProperties.get(key);
        if (isConfigured(serdeValue)) {
            return serdeValue;
        }
        return tableProperties.get(key);
    }

    private boolean isConfigured(String value) {
        return !Strings.isNullOrEmpty(value) && !"none".equalsIgnoreCase(value);
    }

    private String readSchemaUrl(String schemaUrl) {
        Path path = new Path(schemaUrl);
        try {
            FileSystem fs = path.getFileSystem(hadoopConf);
            try (FSDataInputStream inputStream = fs.open(path)) {
                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException(
                    "Failed to read Avro schema url %s. msg: %s", schemaUrl, e.getMessage());
        }
    }

    private String parseSchema(String schemaJson, String source) {
        try {
            return new Schema.Parser().parse(schemaJson).toString();
        } catch (RuntimeException e) {
            throw new StarRocksConnectorException(
                    "Failed to parse Avro schema from %s. msg: %s", source, e.getMessage());
        }
    }
}
