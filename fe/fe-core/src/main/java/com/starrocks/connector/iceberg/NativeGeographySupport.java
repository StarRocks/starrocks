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

package com.starrocks.connector.iceberg;

import com.starrocks.planner.ResultSink;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TResultSinkType;

/** Supported native Iceberg GEOGRAPHY semantics and execution paths. */
public final class NativeGeographySupport {
    private NativeGeographySupport() {
    }

    public static void validateExecution(JobSpec job, boolean spillEnabled) {
        var descriptors = job.getDescTable();
        if (descriptors == null || !descriptors.isSetSlotDescriptors()
                || descriptors.getSlotDescriptors().stream().filter(slot -> slot.isIsMaterialized())
                .flatMap(slot -> slot.getSlotType().getTypes().stream())
                .noneMatch(type -> type.isSetScalar_type()
                        && type.getScalar_type().getType() == TPrimitiveType.GEOGRAPHY)) {
            return;
        }
        // Query plans have one protocol ResultSink; all other fragment sinks exchange rows internally.
        if (spillEnabled || !job.isQueryType() || job.getFragments().stream().noneMatch(fragment ->
                fragment.getSink() instanceof ResultSink sink && sink.getSinkType() == TResultSinkType.MYSQL_PROTOCAL)) {
            throw new SemanticException("Native Iceberg GEOGRAPHY currently supports MySQL query output without spill");
        }
    }
}
