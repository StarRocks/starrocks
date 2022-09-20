// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

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
}
