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
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.type.DateType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.varcharColumn;

class PathPartitionValuesTest {

    private static final String PATH = "s3://bucket/t/dt=2026-09-10/region=us-west/part-0.orc";

    @Test
    void readsThePartitionSourcesExactlyAsTheLoadDoes() throws Exception {
        List<String> columnsFromPath = List.of("dt", "region");
        PathPartitionValues values = PathPartitionValues.of(columnsFromPath,
                List.of(varcharColumn("region"), new Column("dt", DateType.DATE)), List.of("region", "dt"));

        List<String> writtenByLoad = HdfsUtil.parseColumnsFromPath(PATH, columnsFromPath);
        Assertions.assertEquals(List.of(writtenByLoad.get(1), writtenByLoad.get(0)), values.rawValues(PATH));
        Assertions.assertEquals(List.of("us-west", "2026-09-10"), values.rawValues(PATH));
    }

    @Test
    void columnNamesMatchCaseInsensitivelyButPathKeysAsDeclared() throws Exception {
        PathPartitionValues values = PathPartitionValues.of(
                List.of("DT"), List.of(varcharColumn("dt")), List.of("dt"));

        Assertions.assertEquals(List.of("1"), values.rawValues("s3://b/DT=1/f.orc"));
        // The load's parser matches the path key exactly as declared, so a lower-case key is absent.
        Assertions.assertThrows(StarRocksException.class, () -> values.rawValues("s3://b/dt=1/f.orc"));
    }

    @Test
    void aPartitionSourceThatIsNotAPathColumnDisablesIt() {
        Assertions.assertNull(PathPartitionValues.of(
                List.of("dt"), List.of(varcharColumn("dt"), varcharColumn("ts")), List.of("dt", "ts")));
    }

    @Test
    void anUnmappedPartitionSourceDisablesIt() {
        List<String> names = new ArrayList<>();
        names.add(null);
        Assertions.assertNull(PathPartitionValues.of(List.of("dt"), List.of(varcharColumn("dt")), names));
    }

    @Test
    void noPathColumnsOrNoPartitionSourcesDisablesIt() {
        Assertions.assertNull(PathPartitionValues.of(List.of(), List.of(varcharColumn("dt")), List.of("dt")));
        Assertions.assertNull(PathPartitionValues.of(List.of("dt"), List.of(), List.of()));
    }

    @Test
    void typedValuesConvertLikeASampledCell() {
        PathPartitionValues values = PathPartitionValues.of(
                List.of("dt"), List.of(new Column("dt", DateType.DATE)), List.of("dt"));

        List<Variant> typed = values.typed(List.of("2026-09-10"));

        Assertions.assertEquals(0, typed.get(0).compareTo(Variant.of(DateType.DATE, "2026-09-10")));
        Assertions.assertThrows(RuntimeException.class, () -> values.typed(List.of("")));
    }
}
