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

package com.starrocks.epack.warehouse;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.ExceptionChecker;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class CNGroupStatementExecutorTest extends LocalWarehouseTestBase {

    @BeforeClass
    public static void init() {
        setupBeforeClass();
    }

    @Test
    public void testAddCNGroup() {
        { // warehouse not exist
            String sql = "ALTER WAREHOUSE " + randomWarehouseName() + " ADD CNGROUP cngroup1";
            ErrorReportException exception =
                    Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_WAREHOUSE, exception.getErrorCode());
        }
        { // cngroup name invalid
            String sql = "ALTER WAREHOUSE default_warehouse ADD CNGROUP addcngroup%^1";
            Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
        }
        { // empty cngroup name
            String sql = "ALTER WAREHOUSE default_warehouse ADD CNGROUP ''";
            Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode());
        }
        { // duplicate cngroup name
            String cnGroupName = randomCNGroupName();
            String warehouseName = randomWarehouseName();
            ensureWarehouseCreated(warehouseName);
            ensureCnGroupCreated(warehouseName, cnGroupName);

            // Create a cngroup that the name already exists
            String sql = "ALTER WAREHOUSE " + warehouseName + " ADD CNGROUP " + cnGroupName;
            Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_CNGROUP_EXISTS, connectContext.getState().getErrorCode());

            // No error with 'IF NOT EXISTS'
            String sql2 = "ALTER WAREHOUSE " + warehouseName + " ADD CNGROUP IF NOT EXISTS " + cnGroupName;
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql2));

            ensureCnGroupDropped(warehouseName, cnGroupName);
            ensureWarehouseDropped(warehouseName);
        }

        { // cngroup creation with properties
            String cnGroupName = randomCNGroupName();
            String warehouseName = randomWarehouseName();
            ensureWarehouseCreated(warehouseName);

            // Create a cngroup that the name already exists
            String sql = "ALTER WAREHOUSE " + warehouseName + " ADD CNGROUP " + cnGroupName + " Properties ('location' = 'b')";
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));

            Cluster c = getClusterByName(warehouseName, cnGroupName);
            Assert.assertNotNull(c);
            ExceptionChecker.expectThrowsNoException(() -> {
                Map<String, String> properties = c.getProperties();
                Assert.assertEquals(1, properties.size());
                Assert.assertEquals("b", properties.get("location"));
            });

            ensureCnGroupDropped(warehouseName, cnGroupName);
            ensureWarehouseDropped(warehouseName);
        }
    }

    @Test
    public void testDropCNGroup() {
        { // warehouse not exist
            String sql = "ALTER WAREHOUSE " + randomWarehouseName() + " DROP CNGROUP cngroup1";
            ErrorReportException exception =
                    Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_WAREHOUSE, exception.getErrorCode());
        }
        { // cngroup name invalid
            String sql = "ALTER WAREHOUSE default_warehouse DROP CNGROUP dropcngroup%^1";
            Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
        }
        { // empty cngroup name
            String sql = "ALTER WAREHOUSE default_warehouse DROP CNGROUP ''";
            Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode());
        }
        { // cngroup does not exist
            String cnGroupName = randomCNGroupName();
            String sql = "ALTER WAREHOUSE default_warehouse DROP CNGROUP " + cnGroupName;
            ErrorReportException exception =
                    Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_CNGROUP, exception.getErrorCode());

            // No error with `IF EXISTS`
            String sql2 = "ALTER WAREHOUSE default_warehouse DROP CNGROUP IF EXISTS " + cnGroupName;
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql2));
        }
        { // successfully drop CNGroup
            String cnGroupName = randomCNGroupName();
            String warehouseName = randomWarehouseName();
            ensureWarehouseCreated(warehouseName);
            ensureCnGroupCreated(warehouseName, cnGroupName);

            String sql = "ALTER WAREHOUSE " + warehouseName + " DROP CNGROUP " + cnGroupName;
            ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
            // cluster not exist
            Cluster cluster = getClusterByName(warehouseName, cnGroupName);
            Assert.assertNull(cluster);

            ensureCnGroupDropped(warehouseName, cnGroupName);
            ensureWarehouseDropped(warehouseName);
        }
    }

    @Test
    public void testEnableDisableCNGroup() {
        { // warehouse not exist
            {
                String sql = "ALTER WAREHOUSE " + randomWarehouseName() + " ENABLE CNGROUP cngroup1";
                ErrorReportException exception =
                        Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
                Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_WAREHOUSE, exception.getErrorCode());
            }
            {
                String sql = "ALTER WAREHOUSE " + randomWarehouseName() + " DISABLE CNGROUP cngroup1";
                ErrorReportException exception =
                        Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
                Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_WAREHOUSE, exception.getErrorCode());
            }
        }
        { // cngroup name invalid
            {
                String sql = "ALTER WAREHOUSE default_warehouse ENABLE CNGROUP enablecngroup%^1";
                Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
            }
            {
                String sql = "ALTER WAREHOUSE default_warehouse DISABLE CNGROUP disablecngroup%^1";
                Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
            }
        }
        { // empty cngroup name
            {
                String sql = "ALTER WAREHOUSE default_warehouse ENABLE CNGROUP ''";
                Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
                Assert.assertEquals(sql, ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode());
            }
            {
                String sql = "ALTER WAREHOUSE default_warehouse DISABLE CNGROUP ''";
                Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
                Assert.assertEquals(sql, ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode());
            }
        }
        { // cngroup does not exist
            String cnGroupName = randomCNGroupName();
            {
                String sql = "ALTER WAREHOUSE default_warehouse ENABLE CNGROUP " + cnGroupName;
                ErrorReportException exception =
                        Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
                Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_CNGROUP, exception.getErrorCode());
            }
            {
                String sql = "ALTER WAREHOUSE default_warehouse DISABLE CNGROUP " + cnGroupName;
                ErrorReportException exception =
                        Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
                Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_CNGROUP, exception.getErrorCode());
            }
        }
        { // successfully enable/disable CNGroup
            String cnGroupName = randomCNGroupName();
            String warehouseName = randomWarehouseName();
            ensureWarehouseCreated(warehouseName);
            ensureCnGroupCreated(warehouseName, cnGroupName);

            Cluster c = getClusterByName(warehouseName, cnGroupName);
            Assert.assertNotNull(c);
            {
                String sql = "ALTER WAREHOUSE " + warehouseName + " ENABLE CNGROUP " + cnGroupName;
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                Assert.assertTrue(c.isEnabled());
            }
            {
                String sql = "ALTER WAREHOUSE " + warehouseName + " DISABLE CNGROUP " + cnGroupName;
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                Assert.assertFalse(c.isEnabled());
            }

            ensureCnGroupDropped(warehouseName, cnGroupName);
            ensureWarehouseDropped(warehouseName);
        }
    }

    @Test
    public void testAlterCNGroup() throws DdlException {
        { // warehouse not exist
            String sql = "ALTER WAREHOUSE " + randomWarehouseName() + " MODIFY CNGROUP cngroup1 SET ('a' = 'b')";
            ErrorReportException exception =
                    Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_WAREHOUSE, exception.getErrorCode());
        }
        { // cngroup name invalid
            String sql = "ALTER WAREHOUSE default_warehouse MODIFY CNGROUP altercngroup%^1 SET ('a' = 'b')";
            Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
        }
        { // empty cngroup name
            String sql = "ALTER WAREHOUSE default_warehouse MODIFY CNGROUP '' SET ('a' = 'b')";
            Assert.assertThrows(AnalysisException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_INVALID_CNGROUP_NAME, connectContext.getState().getErrorCode());
        }
        { // cngroup does not exist
            String cnGroupName = randomCNGroupName();
            String sql = "ALTER WAREHOUSE default_warehouse MODIFY CNGROUP " + cnGroupName + " SET ('a' = 'b')";
            ErrorReportException exception =
                    Assert.assertThrows(ErrorReportException.class, () -> starRocksAssert.ddl(sql));
            Assert.assertEquals(sql, ErrorCode.ERR_UNKNOWN_CNGROUP, exception.getErrorCode());
        }
        { // successfully alter CNGroup properties
            String cnGroupName = randomCNGroupName();
            String warehouseName = randomWarehouseName();
            ensureWarehouseCreated(warehouseName);
            ensureCnGroupCreated(warehouseName, cnGroupName);

            Cluster c = getClusterByName(warehouseName, cnGroupName);
            Assert.assertNotNull(c);
            {
                String sql = "ALTER WAREHOUSE " + warehouseName + " MODIFY CNGROUP " + cnGroupName + " SET ('a' = 'b')";
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                Assert.assertEquals("b", c.getProperties().get("a"));
            }
            {
                String sql = "ALTER WAREHOUSE " + warehouseName + " MODIFY CNGROUP " + cnGroupName + " SET ('a' = 'c')";
                ExceptionChecker.expectThrowsNoException(() -> starRocksAssert.ddl(sql));
                Assert.assertEquals("c", c.getProperties().get("a"));
            }

            ensureCnGroupDropped(warehouseName, cnGroupName);
            ensureWarehouseDropped(warehouseName);
        }
    }
}
