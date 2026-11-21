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

package com.starrocks.persist.image;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks actual writer output, including polymorphic objects nested in other messages. */
public final class ProtoInstanceAssertions {
    // Each list follows the stable nonzero Kind numbers, starting at 1; empty names mean no subtype fields.
    static final Map<String, List<String>> INSTANCES_BY_KIND = Map.ofEntries(
            Map.entry("AbstractJobPB", List.of(
                    "backup_job", "restore_job", "lake_backup_job", "lake_restore_job")),
            Map.entry("AlterJobPB", List.of(
                    "rollup_job_v2", "schema_change_job_v2", "optimize_job_v2", "online_optimize_job_v2",
                    "merge_partition_job", "lake_table_schema_change_job", "lake_range_rewrite_schema_change_job",
                    "lake_range_rollup_job", "lake_mv_sort_key_rewrite_job", "lake_table_alter_meta_job",
                    "lake_rollup_job", "lake_table_async_fast_schema_change_job", "lake_table_add_index_job",
                    "lake_table_drop_index_job")),
            Map.entry("AnalyzeJobPB", List.of(
                    "native", "external")),
            Map.entry("AnalyzeStatusPB", List.of(
                    "native", "external")),
            Map.entry("ComputeResourcePB", List.of(
                    "warehouse")),
            Map.entry("DistributionInfoPB", List.of(
                    "hash", "random", "")),
            Map.entry("FunctionPB", List.of(
                    "scalar", "aggregate", "table", "sql")),
            Map.entry("GroupProviderPB", List.of(
                    "", "", "")),
            Map.entry("LoadJobPB", List.of(
                    "broker", "spark", "insert")),
            Map.entry("PartitionInfoPB", List.of(
                    "", "range", "list", "expression_range", "expression_range_v2")),
            Map.entry("PrivilegeObjectPB", List.of(
                    "database", "table", "user", "resource", "table", "table", "global_function", "function", "catalog",
                    "resource_group", "storage_volume", "warehouse", "pipe", "")),
            Map.entry("RecyclePartitionInfoPB", List.of(
                    "range", "range", "", "", "", "")),
            Map.entry("ResourcePB", List.of(
                    "spark_resource", "hive_resource", "iceberg_resource", "hudi_resource", "odbc_catalog_resource",
                    "jdbc_resource")),
            Map.entry("RoutineLoadJobPB", List.of(
                    "kafka", "pulsar")),
            Map.entry("RoutineLoadProgressPB", List.of(
                    "kafka_progress", "pulsar_progress")),
            Map.entry("SecurityIntegrationPB", List.of(
                    "", "", "")),
            Map.entry("SnapshotInfoPB", List.of(
                    "snapshot_info", "lake_table_snapshot_info")),
            Map.entry("TablePB", List.of(
                    "mysql", "olap", "external_olap", "view", "es", "hive", "iceberg", "hudi", "jdbc",
                    "materialized_view", "lake", "file", "lake_materialized_view")),
            Map.entry("TabletPB", List.of(
                    "local", "lake")),
            Map.entry("TvrVersionRangePB", List.of(
                    "", "")),
            Map.entry("TxnCommitAttachmentPB", List.of(
                    "insert", "load_job_final_operation", "manual_load", "mini_load", "routine_load", "stream_load",
                    "replication", "compaction")),
            Map.entry("TypePB", List.of(
                    "scalar", "array", "map", "struct", "", "", "")),
            Map.entry("VariantPB", List.of(
                    "bool_value", "int_value", "large_int_value", "string_value", "date", "decimal_value")));

    static Map<Integer, String> instancesByKind(String messageName) {
        Map<Integer, String> values = new java.util.HashMap<>();
        List<String> sequential = INSTANCES_BY_KIND.get(messageName);
        for (int i = 0; i < sequential.size(); i++) {
            values.put(i + 1, sequential.get(i));
        }
        if (messageName.equals("VariantPB")) {
            values.put(20001, "");
            values.put(20002, "");
            values.put(20003, "");
        } else if (messageName.equals("PrivilegeObjectPB")) {
            values.put(17, "ai_provider");
            values.put(18, "ai_function");
        }
        return values;
    }

    private ProtoInstanceAssertions() {
    }

    public static <P extends Message> P assertInstances(P message) {
        Descriptors.Descriptor descriptor = message.getDescriptorForType();
        List<String> expected = INSTANCES_BY_KIND.get(descriptor.getName());
        Descriptors.OneofDescriptor oneof = descriptor.getRealOneofs().stream()
                .filter(value -> value.getName().equals("instance")).findFirst().orElse(null);
        if (oneof != null) {
            assertNotNull(expected, descriptor.getFullName());
        }
        if (expected != null) {
            Descriptors.EnumValueDescriptor kind =
                    (Descriptors.EnumValueDescriptor) message.getField(descriptor.findFieldByName("kind"));
            String context = descriptor.getName() + "." + kind.getName();
            String instanceName = instancesByKind(descriptor.getName()).get(kind.getNumber());
            assertNotNull(instanceName, context);
            Descriptors.FieldDescriptor instance = oneof == null ? null : message.getOneofFieldDescriptor(oneof);
            if (descriptor.getName().equals("VariantPB") && instance == null) {
                assertTrue(List.of("LARGE_INT", "STRING", "NULL_VALUE", "MINIMUM", "MAXIMUM")
                        .contains(kind.getName()), context);
            } else {
                assertEquals(instanceName, instance == null ? "" : instance.getName(), context);
            }
        }
        for (Map.Entry<Descriptors.FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                if (field.getKey().isRepeated()) {
                    for (Object child : (List<?>) field.getValue()) {
                        assertInstances((Message) child);
                    }
                } else {
                    assertInstances((Message) field.getValue());
                }
            }
        }
        return message;
    }
}
