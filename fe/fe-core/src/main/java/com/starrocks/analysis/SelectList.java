// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SelectList.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Select list items plus distinct clause.
 */
public class SelectList {
    private boolean isDistinct;
    private Map<String, String> optHints;

    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()

    private final List<SelectListItem> items;

    // END: Members that need to be reset()
    // ///////////////////////////////////////

    public SelectList(SelectList other) {
        items = Lists.newArrayList();
        for (SelectListItem item : other.items) {
            items.add(item.clone());
        }
        isDistinct = other.isDistinct;
    }

    public SelectList() {
        items = Lists.newArrayList();
        this.isDistinct = false;
    }

    public SelectList(List<SelectListItem> items, boolean isDistinct) {
        this.isDistinct = isDistinct;
        this.items = items;
    }

    public List<SelectListItem> getItems() {
        return items;
    }

    public void addItem(SelectListItem item) {
        items.add(item);
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setIsDistinct(boolean value) {
        isDistinct = value;
    }

    public Map<String, String> getOptHints() {
        return optHints;
    }

    public void setOptHints(Map<String, String> optHints) {
        this.optHints = optHints;
    }

    public void reset() {
        for (SelectListItem item : items) {
            if (!item.isStar()) {
                item.getExpr().reset();
            }
        }
    }

    @Override
    public SelectList clone() {
        return new SelectList(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SelectList{");
        sb.append("isDistinct=").append(isDistinct);
        sb.append(", selectItems=").append(items.stream().map(v -> v.toSql()).collect(Collectors.toList()));
        sb.append("}");
        return sb.toString();
    }
}
