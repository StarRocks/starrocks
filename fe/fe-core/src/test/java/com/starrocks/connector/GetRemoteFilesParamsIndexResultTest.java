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

import com.starrocks.connector.index.ConnectorIndexResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetRemoteFilesParamsIndexResultTest {
    @Test
    public void testCopiesPreserveRequestSpecificIndexOptions() {
        ConnectorIndexResult result = () -> 7L;
        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder()
                .setPartitionNames(List.of("p0", "p1"))
                .setConnectorIndexResult(result).setDisableGlobalIndex(true).build();
        assertSame(result, params.copy().getConnectorIndexResult());
        assertTrue(params.copy().isDisableGlobalIndex());
        assertSame(result, params.sub(0, 1).getConnectorIndexResult());
        assertTrue(params.sub(0, 1).isDisableGlobalIndex());
        assertFalse(GetRemoteFilesParams.newBuilder().build().isDisableGlobalIndex());
    }
}
