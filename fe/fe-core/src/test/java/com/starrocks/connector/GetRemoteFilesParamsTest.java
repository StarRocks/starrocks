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

package com.starrocks.connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GetRemoteFilesParamsTest {

    @Test
    public void testFileSampleRatioAndSeedDefaultToNoSampling() {
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().build();
        Assertions.assertEquals(1.0, params.getFileSampleRatio());
        Assertions.assertEquals(0L, params.getSampleSeed());
    }

    @Test
    public void testSetFileSampleRatioAndSeedRoundTrip() {
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().build();
        params.setFileSampleRatio(0.5);
        params.setSampleSeed(123L);
        Assertions.assertEquals(0.5, params.getFileSampleRatio());
        Assertions.assertEquals(123L, params.getSampleSeed());
    }

    @Test
    public void testCopyPreservesFileSampleRatioAndSeed() {
        // ExternalSampleStatisticsCollectJob's Iceberg path relies on these two fields surviving a
        // copy() -- IcebergMetadata#getRemoteFilesAsync mutates params in place rather than copying
        // specifically to avoid a different loss (IcebergGetRemoteFilesParams' MOR fields), but
        // sibling code elsewhere in the connector layer does call copy(), and it must not silently
        // drop the sampling knobs either.
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().build();
        params.setFileSampleRatio(0.3);
        params.setSampleSeed(42L);

        GetRemoteFilesParams copied = params.copy();
        Assertions.assertEquals(0.3, copied.getFileSampleRatio());
        Assertions.assertEquals(42L, copied.getSampleSeed());
    }
}
