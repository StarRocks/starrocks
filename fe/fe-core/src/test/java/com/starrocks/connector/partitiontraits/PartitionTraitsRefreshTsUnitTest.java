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

import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.PartitionInfo;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class PartitionTraitsRefreshTsUnitTest {

    /**
     * maxPartitionRefreshTs() must return epoch millis: Hive partition modified times are epoch
     * seconds (transient_lastDdlTime, default SECONDS unit), so they must be converted, otherwise the
     * MV rewrite staleness gap against the wall-clock-millis confirmed baseline is hugely negative
     * and the MV is judged fresh forever.
     */
    @Test
    public void testHiveMaxPartitionRefreshTsNormalizedToMillis() {
        new MockUp<HivePartitionTraits>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
                PartitionInfo p1 = () -> 1_700_000_000L; // epoch seconds
                PartitionInfo p2 = () -> 1_700_000_600L; // epoch seconds, the max
                return ImmutableMap.of("p1", p1, "p2", p2);
            }
        };
        HivePartitionTraits traits = new HivePartitionTraits();
        Assertions.assertEquals(Optional.of(1_700_000_600_000L), traits.maxPartitionRefreshTs());
    }

    /** Paimon already reports millis (unit override): the conversion must be an identity. */
    @Test
    public void testPaimonMaxPartitionRefreshTsIdentity() {
        new MockUp<PaimonPartitionTraits>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
                PartitionInfo p = new PartitionInfo() {
                    @Override
                    public long getModifiedTime() {
                        return 1_700_000_600_000L; // already millis
                    }

                    @Override
                    public TimeUnit getModifiedTimeUnit() {
                        return TimeUnit.MILLISECONDS;
                    }
                };
                return ImmutableMap.of("p", p);
            }
        };
        PaimonPartitionTraits traits = new PaimonPartitionTraits();
        Assertions.assertEquals(Optional.of(1_700_000_600_000L), traits.maxPartitionRefreshTs());
    }

    /**
     * JDBC partition modified times come from java.sql.Timestamp.getTime() (epoch millis) and the
     * Partition class declares MILLISECONDS accordingly: the contract conversion must be an identity.
     */
    @Test
    public void testJdbcMaxPartitionRefreshTsIdentity() {
        new MockUp<JDBCPartitionTraits>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
                return ImmutableMap.of("p",
                        new com.starrocks.connector.jdbc.Partition("p", 1_700_000_600_000L));
            }
        };
        JDBCPartitionTraits traits = new JDBCPartitionTraits();
        Assertions.assertEquals(Optional.of(1_700_000_600_000L), traits.maxPartitionRefreshTs());
    }
}
