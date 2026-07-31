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

package com.starrocks.connector.partitiontraits;

import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.StarRocksPartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorPartitionTraits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StarRocksPartitionTraitsTest {
    @Test
    public void testStarRocksPartitionTraits() {
        StarRocksPartitionTraits traits = new StarRocksPartitionTraits();
        Assertions.assertFalse(traits.isSupportPCTRefresh());

        PartitionKey key = traits.createEmptyKey();
        Assertions.assertTrue(key instanceof StarRocksPartitionKey);

        Assertions.assertTrue(ConnectorPartitionTraits.isSupported(Table.TableType.STARROCKS));
        Assertions.assertFalse(ConnectorPartitionTraits.isSupportPCTRefresh(Table.TableType.STARROCKS));
        Assertions.assertTrue(ConnectorPartitionTraits.build(Table.TableType.STARROCKS)
                instanceof StarRocksPartitionTraits);
    }
}
