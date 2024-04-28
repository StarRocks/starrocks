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

package com.starrocks.connector.delta;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.validate.ValidateException;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_NAME;

public class DeltaUtilsTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testCheckTableFeatureSupported() {
        expectedEx.expect(ValidateException.class);
        expectedEx.expectMessage("Delta table is missing protocol or metadata information.");
        DeltaUtils.checkTableFeatureSupported(null, null);
    }

    @Test
    public void testCheckTableFeatureSupported2(@Mocked Metadata metadata) {
        expectedEx.expect(ValidateException.class);
        expectedEx.expectMessage("Delta table feature [column mapping] is not supported");

        new Expectations(metadata) {
            {
                metadata.getConfiguration();
                result = ImmutableMap.of(COLUMN_MAPPING_MODE_KEY, COLUMN_MAPPING_MODE_NAME);
                minTimes = 0;
            }
        };

        DeltaUtils.checkTableFeatureSupported(new Protocol(3, 7, Lists.newArrayList(),
                Lists.newArrayList()), metadata);
    }

    @Test
    public void testCheckTableFeatureSupported3(@Mocked Metadata metadata, @Mocked Protocol protocol) {
        expectedEx.expect(ValidateException.class);
        expectedEx.expectMessage("Delta table feature [timestampNtz] is not supported");
        new Expectations() {
            {
                metadata.getConfiguration();
                result = ImmutableMap.of(COLUMN_MAPPING_MODE_KEY, "none");
                minTimes = 0;
            }

            {
                protocol.getReaderFeatures();
                result = Lists.newArrayList("timestampNtz");
                minTimes = 0;
            }
        };

        DeltaUtils.checkTableFeatureSupported(new Protocol(3, 7, Lists.newArrayList(),
                Lists.newArrayList()), metadata);
    }
}
