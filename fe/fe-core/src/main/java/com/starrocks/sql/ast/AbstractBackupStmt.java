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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.TableRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AbstractBackupStmt extends DdlStmt {
    public enum BackupObjectType {
        TABLE,
        MV,
        VIEW,
        FUNCTION,
        EXTERNAL_CATALOG,
    }

    protected LabelName labelName;
    protected String repoName;
    protected List<TableRef> tblRefs;
    protected List<FunctionRef> fnRefs;
    protected List<CatalogRef> externalCatalogRefs;

    protected Set<BackupObjectType> allMarker;

    protected boolean withOnClause;

    // In new grammer for RESTORE, user can specify origin DB name
    // in snapshot meta
    protected String originDbName;

    protected Map<String, String> properties;

    protected long timeoutMs;

    public AbstractBackupStmt(LabelName labelName, String repoName, List<TableRef> tableRefs,
                              List<FunctionRef> fnRefs, List<CatalogRef> externalCatalogRefs, Set<BackupObjectType> allMarker,
                              boolean withOnClause, String originDbName, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.labelName = labelName;
        this.repoName = repoName;
        this.tblRefs = tableRefs;
        if (this.tblRefs == null) {
            this.tblRefs = Lists.newArrayList();
        }
        this.fnRefs = fnRefs;
        if (this.fnRefs == null) {
            this.fnRefs = Lists.newArrayList();
        }
        this.externalCatalogRefs = externalCatalogRefs;
        if (this.externalCatalogRefs == null) {
            this.externalCatalogRefs = Lists.newArrayList();
        }
        this.allMarker = allMarker;
        if (this.allMarker == null) {
            this.allMarker = Sets.newHashSet();
        }

        this.originDbName = originDbName;
        this.withOnClause = withOnClause;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public LabelName getLabelName() {
        return labelName;
    }

    public String getRepoName() {
        return repoName;
    }

    public List<TableRef> getTableRefs() {
        return tblRefs;
    }

    public List<FunctionRef> getFnRefs() {
        return fnRefs;
    }

    public List<CatalogRef> getExternalCatalogRefs() {
        return externalCatalogRefs;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean withOnClause() {
        return withOnClause;
    }

    public boolean allFunction() {
        return allMarker.contains(BackupObjectType.FUNCTION);
    }

    public boolean allTable() {
        return allMarker.contains(BackupObjectType.TABLE);
    }

    public boolean allMV() {
        return allMarker.contains(BackupObjectType.MV);
    }

    public boolean allView() {
        return allMarker.contains(BackupObjectType.VIEW);
    }

    public boolean allExternalCatalog() {
        return allMarker.contains(BackupObjectType.EXTERNAL_CATALOG);
    }

    public void setAllExternalCatalog() {
        allMarker.add(BackupObjectType.EXTERNAL_CATALOG);
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public String getOriginDbName() {
        return this.originDbName;
    }

    public boolean containsExternalCatalog() {
        return allExternalCatalog() || !externalCatalogRefs.isEmpty();
    }
}

