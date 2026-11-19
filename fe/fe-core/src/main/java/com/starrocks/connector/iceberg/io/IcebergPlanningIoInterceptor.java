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

package com.starrocks.connector.iceberg.io;

import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.model.S3Request;

/**
 * Counts AWS SDK v2 retries into the {@link IcebergPlanningIoStats} of the calling thread.
 *
 * <p>Registered for every SDK client through the classpath resource
 * {@code software/amazon/awssdk/global/handlers/execution.interceptors}, because iceberg's S3FileIO builds
 * its own client. {@code beforeTransmission} runs once per attempt, inside the SDK's retry loop and on the
 * calling thread for a sync client, so every call after the first one of an execution is a retry. Only S3
 * requests count: a catalog call (Glue, STS) made inside the same scope retrying is not storage I/O. A thread
 * without planning stats installed -- any other SDK use in the FE -- costs one thread-local read.
 */
public class IcebergPlanningIoInterceptor implements ExecutionInterceptor {
    private static final ExecutionAttribute<Boolean> ATTEMPTED =
            new ExecutionAttribute<>("StarRocksIcebergPlanningIoAttempted");

    @Override
    public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
        IcebergPlanningIoStats stats = IcebergPlanningIoStats.current();
        if (stats == null || !(context.request() instanceof S3Request)) {
            return;
        }
        if (Boolean.TRUE.equals(executionAttributes.getAttribute(ATTEMPTED))) {
            stats.recordRetry();
        } else {
            executionAttributes.putAttribute(ATTEMPTED, Boolean.TRUE);
        }
    }
}
