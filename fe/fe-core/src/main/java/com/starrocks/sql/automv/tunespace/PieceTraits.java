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

import com.starrocks.sql.automv.qe.TableInfo;

import java.util.List;
import java.util.Map;

public class PieceTraits {
    // use version to prevent
    private long version;
    private String name;
    private boolean isNonSPJG;
    // dimensions and metrics
    private int numDimensions;
    private int numRollupDimensions;
    private int numMetrics;
    private int numDistinctMetrics;
    // group-by columns and aggregations functions
    private Boolean allRollupAble;
    private Boolean rollupConvertible;
    private List<String> rollupAbleAggs;
    private List<String> rollupConvertibleAggs;
    private List<String> rollupUnableAggs;
    // hoistedConjuncts;
    private int numHoistedConjuncts;
    private List<String> hoistedConjuncts;
    private LegacyMVInfo legacyMV;
    private Map<String, TableInfo> tables;

    public boolean isNonSPJG() {
        return isNonSPJG;
    }

    public void setNonSPJG(boolean nonSPJG) {
        isNonSPJG = nonSPJG;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public int getNumDimensions() {
        return numDimensions;
    }

    public void setNumDimensions(int numDimensions) {
        this.numDimensions = numDimensions;
    }

    public int getNumRollupDimensions() {
        return numRollupDimensions;
    }

    public void setNumRollupDimensions(int numRollupDimensions) {
        this.numRollupDimensions = numRollupDimensions;
    }

    public int getNumMetrics() {
        return numMetrics;
    }

    public void setNumMetrics(int numMetrics) {
        this.numMetrics = numMetrics;
    }

    public int getNumDistinctMetrics() {
        return numDistinctMetrics;
    }

    public void setNumDistinctMetrics(int numDistinctMetrics) {
        this.numDistinctMetrics = numDistinctMetrics;
    }

    public Boolean isRollupAble() {
        return allRollupAble;
    }

    public void setAllRollupAble(boolean allRollupAble) {
        this.allRollupAble = allRollupAble;
    }

    public Boolean isRollupConvertible() {
        return rollupConvertible;
    }

    public void setRollupConvertible(boolean rollupConvertible) {
        this.rollupConvertible = rollupConvertible;
    }

    public List<String> getRollupAbleAggs() {
        return rollupAbleAggs;
    }

    public void setRollupAbleAggs(List<String> rollupAbleAggs) {
        this.rollupAbleAggs = rollupAbleAggs;
    }

    public List<String> getRollupConvertibleAggs() {
        return rollupConvertibleAggs;
    }

    public void setRollupConvertibleAggs(List<String> rollupConvertibleAggs) {
        this.rollupConvertibleAggs = rollupConvertibleAggs;
    }

    public List<String> getRollupUnableAggs() {
        return rollupUnableAggs;
    }

    public void setRollupUnableAggs(List<String> rollupUnableAggs) {
        this.rollupUnableAggs = rollupUnableAggs;
    }

    public int getNumHoistedConjuncts() {
        return numHoistedConjuncts;
    }

    public void setNumHoistedConjuncts(int numHoistedConjuncts) {
        this.numHoistedConjuncts = numHoistedConjuncts;
    }

    public List<String> getHoistedConjuncts() {
        return hoistedConjuncts;
    }

    public void setHoistedConjuncts(List<String> hoistedConjuncts) {
        this.hoistedConjuncts = hoistedConjuncts;
    }

    public LegacyMVInfo getLegacyMV() {
        return legacyMV;
    }

    public void setLegacyMV(LegacyMVInfo legacyMV) {
        this.legacyMV = legacyMV;
    }

    public Map<String, TableInfo> getTables() {
        return tables;
    }

    public void setTables(Map<String, TableInfo> tables) {
        this.tables = tables;
    }
}