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

package com.starrocks.connector.persist;

import com.google.common.collect.Sets;
import com.starrocks.catalog.MvId;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ConnectorTblPersistInfoMgrTest {
    @Test
    public void testBasic() throws Exception {
        ConnectorTblPersistInfoMgr connectorTblPersistInfoMgr = new ConnectorTblPersistInfoMgr();

        connectorTblPersistInfoMgr.addConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl1", ConnectorTableInfo.builder().
                        setRelatedMaterializedViews(Sets.newHashSet(new MvId(1, 2))).build());
        connectorTblPersistInfoMgr.addConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl2", ConnectorTableInfo.builder().
                        setRelatedMaterializedViews(Sets.newHashSet(new MvId(1, 3))).build());
        connectorTblPersistInfoMgr.addConnectorTableInfo("test_catalog2", "test_db2",
                "test_tbl2", ConnectorTableInfo.builder().
                        setRelatedMaterializedViews(Sets.newHashSet(new MvId(2, 5))).build());
        Assert.assertEquals(2, connectorTblPersistInfoMgr.getChecksum());

        Assert.assertEquals(connectorTblPersistInfoMgr.getConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl1").getRelatedMaterializedViews(), Sets.newHashSet(new MvId(1, 2)));
        Assert.assertEquals(connectorTblPersistInfoMgr.getConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl2").getRelatedMaterializedViews(), Sets.newHashSet(new MvId(1, 3)));
        Assert.assertEquals(connectorTblPersistInfoMgr.getConnectorTableInfo("test_catalog2", "test_db2",
                "test_tbl2").getRelatedMaterializedViews(), Sets.newHashSet(new MvId(2, 5)));
    }

    @Test
    public void testSerialization() throws IOException {
        ConnectorTblPersistInfoMgr connectorTblPersistInfoMgr = new ConnectorTblPersistInfoMgr();

        connectorTblPersistInfoMgr.addConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl1", ConnectorTableInfo.builder().build());
        connectorTblPersistInfoMgr.addConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl2", ConnectorTableInfo.builder().build());
        connectorTblPersistInfoMgr.addConnectorTableInfo("test_catalog2", "test_db1",
                "test_tbl2", ConnectorTableInfo.builder().build());
        connectorTblPersistInfoMgr.addConnectorTableInfo("test_catalog2", "test_db2",
                "test_tbl2", ConnectorTableInfo.builder().build());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);

        long checksum = 111;
        long imageChecksum = connectorTblPersistInfoMgr.saveConnectorTblPersistInfoMgr(dataOutputStream, checksum);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        ConnectorTblPersistInfoMgr newConnectorTblPersistInfoMgr = ConnectorTblPersistInfoMgr.
                loadConnectorTblPersistInfoMgr(dataInputStream);
        checksum ^= newConnectorTblPersistInfoMgr.getChecksum();
        Assert.assertEquals(checksum, imageChecksum);
        Assert.assertEquals(3, newConnectorTblPersistInfoMgr.getChecksum());

        Assert.assertNotNull(newConnectorTblPersistInfoMgr.getConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl1"));
        Assert.assertNotNull(newConnectorTblPersistInfoMgr.getConnectorTableInfo("test_catalog1", "test_db1",
                "test_tbl2"));
        Assert.assertNotNull(newConnectorTblPersistInfoMgr.getConnectorTableInfo("test_catalog2", "test_db1",
                "test_tbl2"));
        Assert.assertNotNull(newConnectorTblPersistInfoMgr.getConnectorTableInfo("test_catalog2", "test_db2",
                "test_tbl2"));
    }
}
