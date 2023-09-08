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
package com.starrocks.connector.delta.cache;

import java.io.Serializable;
import java.util.Objects;

public class DeltaLakeTableName implements Serializable {
    private String catalog;
    private String dbName;
    private String tblName;
    private String dataPath;

    public DeltaLakeTableName(String catalog, String dbName, String tblName, String dataPath) {
        this.catalog = catalog;
        this.dbName = dbName;
        this.tblName = tblName;
        this.dataPath = dataPath;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTblName() {
        return tblName;
    }

    public void setTblName(String tblName) {
        this.tblName = tblName;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    @Override
    public String toString() {
        return catalog + '.' + dbName + '.' + tblName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeltaLakeTableName that = (DeltaLakeTableName) o;
        return Objects.equals(catalog, that.catalog) &&
                Objects.equals(dbName, that.dbName) &&
                Objects.equals(tblName, that.tblName) &&
                Objects.equals(dataPath, that.dataPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, dbName, tblName, dataPath);
    }
}