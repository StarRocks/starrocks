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

package com.starrocks.connector.paimon;

import com.starrocks.sql.analyzer.SemanticException;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Native schema copying for CREATE TABLE LIKE, without SHOW CREATE or the read-side type conversion. */
public final class PaimonTableLikeHelper {
    private PaimonTableLikeHelper() {
    }

    /**
     * Copy only the schema of an already loaded native table. The new table receives new field IDs and its own
     * location/identity from the target catalog; no snapshots, files, or source storage handles are copied.
     */
    public static Schema copySchema(FileStoreTable source, Map<String, String> overrides) {
        TableSchema sourceSchema = source.schema();
        Map<String, String> options = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : sourceSchema.options().entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (key.equals(CoreOptions.PRIMARY_KEY.key())) {
                checkKeyOption(key, entry.getValue(), sourceSchema.primaryKeys());
            } else if (key.equals(CoreOptions.PARTITION.key())) {
                checkKeyOption(key, entry.getValue(), sourceSchema.partitionKeys());
            } else if (!isSourceSpecificOption(key)) {
                // Keep native field/format/merge options too: the CREATE converter's allowlist is for user input,
                // not a reason to drop valid persisted native table semantics.
                options.put(entry.getKey(), entry.getValue());
            }
        }
        Map<String, String> validatedOverrides = PaimonTableSchemaConverter.tableOptions(overrides);
        for (Map.Entry<String, String> entry : validatedOverrides.entrySet()) {
            if (entry.getKey().equals(CoreOptions.PRIMARY_KEY.key())) {
                checkKeyOption(entry.getKey(), entry.getValue(), sourceSchema.primaryKeys());
            } else if (isSourceSpecificOption(entry.getKey())) {
                throw new SemanticException("Paimon CREATE TABLE LIKE cannot override source-specific property: %s",
                        entry.getKey());
            } else {
                options.put(entry.getKey(), entry.getValue());
            }
        }

        try {
            Schema.Builder builder = Schema.newBuilder();
            for (DataField field : sourceSchema.fields()) {
                // The native DataType retains nested nullability, timestamp kind/precision, and nested defaults.
                // Builder reassigns IDs recursively for a new table, including IDs inside arrays/maps/rows.
                builder.column(field.name(), field.type(), field.description(), field.defaultValue());
            }
            Schema schema = builder.partitionKeys(sourceSchema.partitionKeys()).primaryKey(sourceSchema.primaryKeys())
                    .options(options).comment(sourceSchema.comment()).build();
            SchemaValidation.validateTableSchema(new TableSchema(0, schema.fields(), builder.getHighestFieldId(),
                    schema.partitionKeys(), schema.primaryKeys(), schema.options(), schema.comment()));
            return schema;
        } catch (RuntimeException e) {
            // SDK diagnostics can include entire option maps. Do not expose source locations or credentials.
            throw new SemanticException("Invalid Paimon CREATE TABLE LIKE schema or table options");
        }
    }

    private static void checkKeyOption(String name, String value, List<String> keys) {
        String[] names = value == null ? new String[0] : value.split(",", -1);
        if (names.length != keys.size()) {
            throw new SemanticException("Paimon CREATE TABLE LIKE cannot change %s columns", name);
        }
        for (int i = 0; i < names.length; i++) {
            if (!names[i].trim().equalsIgnoreCase(keys.get(i))) {
                throw new SemanticException("Paimon CREATE TABLE LIKE cannot change %s columns", name);
            }
        }
    }

    private static boolean isSourceSpecificOption(String key) {
        // Persisted per-column options are schema semantics, even when a column is named path, token, etc.
        if (key.startsWith(CoreOptions.FIELDS_PREFIX + ".")) {
            return false;
        }
        return key.equals("type") || key.equals("owner") || key.equals("warehouse") || key.equals("branch")
                || key.equals("metastore") || key.equals("numrows") || key.equals("numfiles")
                || key.equals("totalsize") || key.equals("lastupdatetime") || key.equals("totalbuckets")
                || key.equals("location") || key.endsWith(".location") || key.contains("path")
                || key.equals("uuid") || key.endsWith(".uuid") || key.endsWith("-uuid") || key.endsWith("_uuid")
                || key.equals("table-id") || key.equals("table.id") || key.equals("table_id")
                || key.equals("schema-id") || key.equals("schema.id") || key.equals("schema_id")
                || key.equals("object-id") || key.equals("object.id") || key.equals("object_id")
                || key.contains("password") || key.contains("secret") || key.contains("credential") || key.contains("token")
                || key.contains("access-key") || key.contains("access_key")
                || key.startsWith("catalog.") || key.startsWith("paimon.catalog.") || key.startsWith("hive.")
                || key.startsWith("hadoop.") || key.startsWith("fs.") || key.startsWith("s3.") || key.startsWith("s3a.")
                || key.startsWith("oss.") || key.startsWith("azure.") || key.startsWith("gcs.")
                || key.startsWith("format-table.") || key.startsWith("scan.") || key.startsWith("consumer.");
    }
}
