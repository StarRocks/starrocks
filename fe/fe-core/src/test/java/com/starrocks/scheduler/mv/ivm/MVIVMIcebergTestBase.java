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

package com.starrocks.scheduler.mv.ivm;

import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeAll;

import java.util.Optional;

public class MVIVMIcebergTestBase extends MVIVMTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Override
    public void advanceTableVersionTo(long toVersion) {
        new MockUp<MockIcebergMetadata>() {
            @Mock
            public TvrTableSnapshot getCurrentTvrSnapshot(String dbName, com.starrocks.catalog.Table table) {
                return TvrTableSnapshot.of(Optional.of(toVersion));
            }
        };
    }
}
