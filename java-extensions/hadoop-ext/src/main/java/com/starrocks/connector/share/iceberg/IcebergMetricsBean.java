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

package com.starrocks.connector.share.iceberg;

import java.io.Serializable;
import java.util.Map;

public class IcebergMetricsBean implements Serializable {
    private Map<Integer, Long> columnSizes = null;
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullValueCounts = null;
    private Map<Integer, Long> nanValueCounts = null;
    private Map<Integer, byte[]> lowerBounds = null;
    private Map<Integer, byte[]> upperBounds = null;

    public IcebergMetricsBean() {
    }

    public Map<Integer, Long> getColumnSizes() {
        return columnSizes;
    }

    public void setColumnSizes(Map<Integer, Long> columnSizes) {
        this.columnSizes = columnSizes;
    }

    public Map<Integer, Long> getValueCounts() {
        return valueCounts;
    }

    public void setValueCounts(Map<Integer, Long> valueCounts) {
        this.valueCounts = valueCounts;
    }

    public Map<Integer, Long> getNullValueCounts() {
        return nullValueCounts;
    }

    public void setNullValueCounts(Map<Integer, Long> nullValueCounts) {
        this.nullValueCounts = nullValueCounts;
    }

    public Map<Integer, Long> getNanValueCounts() {
        return nanValueCounts;
    }

    public void setNanValueCounts(Map<Integer, Long> nanValueCounts) {
        this.nanValueCounts = nanValueCounts;
    }

    public Map<Integer, byte[]> getLowerBounds() {
        return lowerBounds;
    }

    public void setLowerBounds(Map<Integer, byte[]> lowerBounds) {
        this.lowerBounds = lowerBounds;
    }

    public Map<Integer, byte[]> getUpperBounds() {
        return upperBounds;
    }

    public void setUpperBounds(Map<Integer, byte[]> upperBounds) {
        this.upperBounds = upperBounds;
    }
}
