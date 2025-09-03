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


package com.starrocks.sql.ast;

import com.starrocks.catalog.DataProperty;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TTabletType;
import org.apache.commons.lang.NotImplementedException;

import java.util.Map;

public class PartitionDesc implements ParseNode {

    protected final NodePosition pos;
    protected boolean isSystem = false;

    public PartitionDesc() {
        this(NodePosition.ZERO);
    }

    protected PartitionDesc(NodePosition pos) {
        this.pos = pos;
    }

    public String toSql() {
        throw new NotImplementedException();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public String getPartitionName() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public boolean isSetIfNotExists() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public Map<String, String> getProperties() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public short getReplicationNum() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public DataProperty getPartitionDataProperty() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public Long getVersionInfo() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public TTabletType getTabletType() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public boolean isInMemory() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public DataCacheInfo getDataCacheInfo() throws NotImplementedException {
        throw new NotImplementedException();
    }

    public boolean isSystem() {
        return isSystem;
    }

    public void setSystem(boolean system) {
        isSystem = system;
    }
}