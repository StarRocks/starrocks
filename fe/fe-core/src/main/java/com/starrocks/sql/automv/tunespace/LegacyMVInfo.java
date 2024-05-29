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

package com.starrocks.sql.automv.tunespace;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class LegacyMVInfo {
    private String mvCatalogName;
    private String mvDatabaseName;
    private String mvName;
    private List<String> mvColumns;
    private List<String> mvColumnComments;
    private List<String> mvIndices;
    private List<List<String>> mvIndexColumns;
    private String mvComment;
    private String mvPartition;
    private List<String> mvPartitionKey;
    private String mvDistribution;
    private int mvBucketNum;
    private List<String> mvBucketKey;
    private List<String> mvSortKey;
    private List<String> mvRefreshScheme;
    private Map<String, String> mvSimpleProperties;
    private Set<String> mvBfColumns;
    private Map<String, String> mvBfProperties;
    private Map<String, String> mvColocateProperties;
    private List<List<String>> mvUniqueKeyColumns;
    private List<String> mvUniqueKeys;
    private List<List<String>> mvForeignKeyColumns;
    private List<String> mvForeignKeys;
    private Map<String, String> mvUniqueKeyProperties;
    private Map<String, String> mvForeignKeyProperties;

    public static LegacyMVInfo from(MaterializedViewPlus mvPlus) {
        LegacyMVInfo legacyMv = new LegacyMVInfo();
        legacyMv.setMvCatalogName(mvPlus.getFqName().getCatalog());
        legacyMv.setMvDatabaseName(mvPlus.getFqName().getDb());
        legacyMv.setMvName(mvPlus.getFqName().getTbl());
        legacyMv.setMvColumns(mvPlus.getColumns());
        legacyMv.setMvColumnComments(mvPlus.getColumnComments());
        legacyMv.setMvIndices(mvPlus.getIndices());
        legacyMv.setMvIndexColumns(mvPlus.getIndexColumns());
        legacyMv.setMvComment(mvPlus.getComment().orElse(null));
        legacyMv.setMvPartition(mvPlus.getPartition().orElse(null));
        legacyMv.setMvPartitionKey(mvPlus.getPartitionKey());
        legacyMv.setMvDistribution(mvPlus.getDistribution());
        legacyMv.setMvDistributionKey(mvPlus.getDistributionKey());
        legacyMv.setMvBucketNum(mvPlus.getBucketNum());
        legacyMv.setMvSortKey(mvPlus.getSortKey());
        legacyMv.setMvSimpleProperties(mvPlus.getSimpleProperties());
        legacyMv.setMvBfColumns(mvPlus.getBfColumns());
        legacyMv.setMvBfProperties(mvPlus.getBfProperties(mvPlus.getBfColumns()));
        legacyMv.setMvColocateProperties(mvPlus.getColocateProperties());
        legacyMv.setMvUniqueKeyColumns(mvPlus.getUniqueKeyColumns());
        legacyMv.setMvUniqueKeys(mvPlus.getUniqueKeys());
        legacyMv.setMvUniqueKeyProperties(mvPlus.getUniqueKeyProperties(s -> true));
        legacyMv.setMvForeignKeyColumns(mvPlus.getForeignKeyColumns());
        legacyMv.setMvForeignKeys(mvPlus.getForeignKeys());
        legacyMv.setMvForeignKeyProperties(mvPlus.getUniqueKeyProperties(s -> true));
        return legacyMv;
    }

    public String getMvCatalogName() {
        return mvCatalogName;
    }

    public void setMvCatalogName(String mvCatalogName) {
        this.mvCatalogName = mvCatalogName;
    }

    public String getMvDatabaseName() {
        return mvDatabaseName;
    }

    public void setMvDatabaseName(String mvDatabaseName) {
        this.mvDatabaseName = mvDatabaseName;
    }

    public String getMvName() {
        return mvName;
    }

    public void setMvName(String mvName) {
        this.mvName = mvName;
    }

    public Set<String> getMvBfColumns() {
        return mvBfColumns;
    }

    public void setMvBfColumns(Set<String> mvBfColumns) {
        this.mvBfColumns = mvBfColumns;
    }

    public List<List<String>> getMvUniqueKeyColumns() {
        return mvUniqueKeyColumns;
    }

    public void setMvUniqueKeyColumns(List<List<String>> mvUniqueKeyColumns) {
        this.mvUniqueKeyColumns = mvUniqueKeyColumns;
    }

    public List<String> getMvUniqueKeys() {
        return mvUniqueKeys;
    }

    public void setMvUniqueKeys(List<String> mvUniqueKeys) {
        this.mvUniqueKeys = mvUniqueKeys;
    }

    public List<List<String>> getMvForeignKeyColumns() {
        return mvForeignKeyColumns;
    }

    public void setMvForeignKeyColumns(List<List<String>> mvForeignKeyColumns) {
        this.mvForeignKeyColumns = mvForeignKeyColumns;
    }

    public List<String> getMvForeignKeys() {
        return mvForeignKeys;
    }

    public void setMvForeignKeys(List<String> mvForeignKeys) {
        this.mvForeignKeys = mvForeignKeys;
    }

    public List<String> getMvColumns() {
        return mvColumns;
    }

    public void setMvColumns(List<String> mvColumns) {
        this.mvColumns = mvColumns;
    }

    public List<String> getMvColumnComments() {
        return mvColumnComments;
    }

    public void setMvColumnComments(List<String> mvColumnComments) {
        this.mvColumnComments = mvColumnComments;
    }

    public List<String> getMvIndices() {
        return mvIndices;
    }

    public void setMvIndices(List<String> mvIndices) {
        this.mvIndices = mvIndices;
    }

    public List<List<String>> getMvIndexColumns() {
        return mvIndexColumns;
    }

    public void setMvIndexColumns(List<List<String>> mvIndexColumns) {
        this.mvIndexColumns = mvIndexColumns;
    }

    public String getMvComment() {
        return mvComment;
    }

    public void setMvComment(String mvComment) {
        this.mvComment = mvComment;
    }

    public String getMvPartition() {
        return mvPartition;
    }

    public void setMvPartition(String mvPartition) {
        this.mvPartition = mvPartition;
    }

    public List<String> getMvPartitionKey() {
        return mvPartitionKey;
    }

    public void setMvPartitionKey(List<String> mvPartitionKey) {
        this.mvPartitionKey = mvPartitionKey;
    }

    public String getMvDistribution() {
        return mvDistribution;
    }

    public void setMvDistribution(String mvDistribution) {
        this.mvDistribution = mvDistribution;
    }

    public int getMvBucketNum() {
        return mvBucketNum;
    }

    public void setMvBucketNum(int mvBucketNum) {
        this.mvBucketNum = mvBucketNum;
    }

    public List<String> getMvBucketKey() {
        return mvBucketKey;
    }

    public void setMvDistributionKey(List<String> mvBucketKey) {
        this.mvBucketKey = mvBucketKey;
    }

    public List<String> getMvSortKey() {
        return mvSortKey;
    }

    public void setMvSortKey(List<String> mvSortKey) {
        this.mvSortKey = mvSortKey;
    }

    public List<String> getMvRefreshScheme() {
        return mvRefreshScheme;
    }

    public void setMvRefreshScheme(List<String> mvRefreshScheme) {
        this.mvRefreshScheme = mvRefreshScheme;
    }

    public Map<String, String> getMvSimpleProperties() {
        return mvSimpleProperties;
    }

    public void setMvSimpleProperties(Map<String, String> mvSimpleProperties) {
        this.mvSimpleProperties = mvSimpleProperties;
    }

    public Map<String, String> getMvBfProperties() {
        return mvBfProperties;
    }

    public void setMvBfProperties(Map<String, String> mvBfProperties) {
        this.mvBfProperties = mvBfProperties;
    }

    public Map<String, String> getMvColocateProperties() {
        return mvColocateProperties;
    }

    public void setMvColocateProperties(Map<String, String> mvColocateProperties) {
        this.mvColocateProperties = mvColocateProperties;
    }

    public Map<String, String> getMvUniqueKeyProperties() {
        return mvUniqueKeyProperties;
    }

    public void setMvUniqueKeyProperties(Map<String, String> mvUniqueKeyProperties) {
        this.mvUniqueKeyProperties = mvUniqueKeyProperties;
    }

    public Map<String, String> getMvForeignKeyProperties() {
        return mvForeignKeyProperties;
    }

    public void setMvForeignKeyProperties(Map<String, String> mvForeignKeyProperties) {
        this.mvForeignKeyProperties = mvForeignKeyProperties;
    }
}
