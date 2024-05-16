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
import com.starrocks.analysis.HintNode;

import java.util.List;

/**
 * Select list items plus distinct clause.
 */
public class SelectList {
    private boolean isDistinct;

    private List<HintNode> hintNodes;

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

    public List<HintNode> getHintNodes() {
        return hintNodes;
    }

    public void setHintNodes(List<HintNode> hintNodes) {
        this.hintNodes = hintNodes;
    }
}
