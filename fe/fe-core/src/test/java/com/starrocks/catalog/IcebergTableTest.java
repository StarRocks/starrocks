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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.server.IcebergTableFactory;
import org.junit.Test;

import java.util.List;

import static com.starrocks.catalog.Type.INT;

public class IcebergTableTest extends TableTestBase {

    @Test(expected = DdlException.class)
    public void testValidateIcebergColumnType() throws DdlException {
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergTable oTable = new IcebergTable(1, "srTableName", "iceberg_catalog",
                "resource_name", "iceberg_db", "iceberg_table", columns, mockedNativeTableB, Maps.newHashMap());
        List<Column> inputColumns = Lists.newArrayList(new Column("k1", INT, true));
        IcebergTableFactory.validateIcebergColumnType(inputColumns, oTable);
    }
}
