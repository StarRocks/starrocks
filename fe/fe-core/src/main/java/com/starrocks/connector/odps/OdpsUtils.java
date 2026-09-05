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

package com.starrocks.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.security.SecurityManager;
import com.starrocks.catalog.OdpsTable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OdpsUtils {
    public static Table getOdpsTable(Odps odps, OdpsTable odpsTable) {
        return odps.tables().get(odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName());
    }

    public static Table getOdpsTable(Odps odps, OdpsTableName odpsTableName) {
        return odps.tables().get(odpsTableName.getDatabaseName(), odpsTableName.getTableName());
    }

    public static Table getOdpsTable(Odps odps, com.starrocks.catalog.Table table) {
        return odps.tables().get(table.getCatalogDBName(), table.getCatalogTableName());
    }

    public static Iterator<Table> getOdpsTablesIterator(Odps odps, String dbName) {
        return odps.tables().iterator(dbName);
    }

    public static List<Partition> getOdpsTablePartitions(Odps odps, OdpsTable odpsTable) {
        return odps.tables().get(odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName()).getPartitions();
    }

    public static List<Partition> getOdpsTablePartitions(Odps odps, OdpsTableName odpsTableName) {
        return odps.tables().get(odpsTableName.getDatabaseName(), odpsTableName.getTableName()).getPartitions();
    }

    public static List<Partition> getOdpsTablePartitionsByNames(Odps odps, OdpsTableName odpsTableName,
                                                                List<String> partitionNames) {
        Table table = odps.tables().get(odpsTableName.getDatabaseName(), odpsTableName.getTableName());
        List<Partition> result = new ArrayList<>(partitionNames.size());
        for (String partitionName : partitionNames) {
            Partition partition = table.getPartition(new PartitionSpec(partitionName));
            if (partition != null) {
                result.add(partition);
            }
        }
        return result;
    }

    public static OdpsTableName getOdpsTableName(OdpsTable odpsTable) {
        return OdpsTableName.of(
                odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName());
    }

    public static OdpsTableName getOdpsTableName(com.starrocks.catalog.Table odpsTable) {
        return OdpsTableName.of(
                odpsTable.getCatalogDBName(), odpsTable.getCatalogTableName());
    }

    public static String getEndpoint(Odps odps) {
        return odps.getEndpoint();
    }

    public static Account getAccount(Odps odps) {
        return odps.getAccount();
    }

    public static SecurityManager getSecurityManager(Odps odps) throws OdpsException {
        return odps.projects().get().getSecurityManager();
    }

    public static Iterator<Project> getProjectIterator(Odps odps, String owner) {
        return odps.projects().iterator(owner);
    }

    public static String getDefaultProject(Odps odps) {
        return odps.getDefaultProject();
    }
}
