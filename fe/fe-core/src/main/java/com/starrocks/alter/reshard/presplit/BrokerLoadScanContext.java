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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.BrokerDesc;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link ScanContext} concrete for the Broker Load integration. The sampler
 * executors that consume this context (meta-tier row-group statistics provider,
 * data-tier sub-query executor) build their own {@code FileScanNode} from the
 * {@link BrokerDesc}, the {@link BrokerFileGroup} list, and the
 * {@link ComputeResource}; the pipeline itself does not introspect.
 *
 * <p>{@code fileStatusesPerGroup} carries the file-status snapshot the load
 * pending task already resolved, parallel to {@link #fileGroups()}. The meta
 * tier reads exactly this snapshot rather than re-globbing — re-listing would
 * race with the load's own enumeration and risk planning quantile cuts
 * from a different file set than the one actually loaded.
 *
 * <p>The {@code brokerDesc} may be {@code null} for HDFS-style direct loads
 * that don't go through a broker — matching the same nullability rule the
 * load planner already follows.
 *
 * <p>{@code loadTimeZone} is the load session timezone (the same value the BE
 * query globals use). The meta-tier readers use it to reproduce the BE's
 * offset for a UTC-adjusted / TIMESTAMP_INSTANT value; a non-fixed / null
 * zone -> data tier.
 * The data-tier query uses the same zone through a {@code SET_VAR} hint.
 *
 * <p>{@code targetBaseSchema} is the target table's base schema as the hook
 * snapshotted it. Only the CSV data tier reads it: a file group that declares
 * no {@code COLUMNS} list takes its positional field layout from that schema
 * ({@link com.starrocks.load.Load#initColumns}), and the sampler has to declare
 * the same layout to FILES. Parquet/ORC resolve by name and never consult it.
 *
 * <p>{@code targetToConstantSql} / {@code targetToExpressionSql} carry each sampled generated
 * column (lower-cased name) as the expression the data tier evaluates in its place -- its
 * definition over the file columns it reads, each cast to its target type, which is how
 * {@code Load} computes it -- and {@code generatedColumnInputs} the target columns those
 * expressions read, which a CSV sample has to declare. All three are empty when no sampled
 * column is generated.
 *
 * <p>{@code sessionSemantics} are the job session's variables that decide how those expressions
 * evaluate and how a timestamp decodes, copied when the hook ran; the data tier sets them on its own
 * session.
 */
public record BrokerLoadScanContext(
        BrokerDesc brokerDesc,
        List<BrokerFileGroup> fileGroups,
        List<List<TBrokerFileStatus>> fileStatusesPerGroup,
        ComputeResource computeResource,
        String loadTimeZone,
        List<Column> targetBaseSchema,
        Map<String, String> targetToConstantSql,
        Map<String, String> targetToExpressionSql,
        List<Column> generatedColumnInputs,
        SampleSessionSemantics sessionSemantics) implements ScanContext {

    public BrokerLoadScanContext {
        Objects.requireNonNull(fileGroups, "fileGroups");
        Objects.requireNonNull(fileStatusesPerGroup, "fileStatusesPerGroup");
        Objects.requireNonNull(computeResource, "computeResource");
        Objects.requireNonNull(targetBaseSchema, "targetBaseSchema");
        Objects.requireNonNull(targetToConstantSql, "targetToConstantSql");
        Objects.requireNonNull(targetToExpressionSql, "targetToExpressionSql");
        Objects.requireNonNull(generatedColumnInputs, "generatedColumnInputs");
        Objects.requireNonNull(sessionSemantics, "sessionSemantics");
        if (fileGroups.size() != fileStatusesPerGroup.size()) {
            throw new IllegalArgumentException(String.format(
                    "fileGroups size %d != fileStatusesPerGroup size %d",
                    fileGroups.size(), fileStatusesPerGroup.size()));
        }
    }

    /**
     * Constructor for the paths that never need the target schema: every format but CSV, and
     * every CSV file group that declares its own {@code COLUMNS} list. A CSV file group without
     * one is rejected rather than sampled against an empty schema.
     */
    public BrokerLoadScanContext(
            BrokerDesc brokerDesc,
            List<BrokerFileGroup> fileGroups,
            List<List<TBrokerFileStatus>> fileStatusesPerGroup,
            ComputeResource computeResource,
            String loadTimeZone) {
        this(brokerDesc, fileGroups, fileStatusesPerGroup, computeResource, loadTimeZone, List.of(),
                Map.of(), Map.of(), List.of(), SampleSessionSemantics.NONE);
    }
}
