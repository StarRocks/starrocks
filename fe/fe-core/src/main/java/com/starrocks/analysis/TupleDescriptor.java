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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/TupleDescriptor.java

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

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.thrift.TTupleDescriptor;

import java.util.ArrayList;
import java.util.List;

@Deprecated
public class TupleDescriptor {
    private final TupleId id;
    private final String debugName; // debug only
    private final ArrayList<SlotDescriptor> slots;

    // underlying table, if there is one
    private Table table;
    // underlying table, if there is one
    private TableRef ref;

    // All legal aliases of this tuple.
    private String[] aliases_;

    // If true, requires that aliases_.length() == 1. However, aliases_.length() == 1
    // does not imply an explicit alias because nested collection refs have only a
    // single implicit alias.
    private boolean hasExplicitAlias_;

    // if false, this tuple doesn't need to be materialized
    private boolean isMaterialized = true;

    public TupleDescriptor(TupleId id) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.debugName = "";
    }

    public TupleDescriptor(TupleId id, String debugName) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.debugName = debugName;
    }

    public void addSlot(SlotDescriptor desc) {
        slots.add(desc);
    }

    public TupleId getId() {
        return id;
    }

    public TableRef getRef() {
        return ref;
    }

    public void setRef(TableRef new_ref) {
        ref = new_ref;
    }

    public ArrayList<SlotDescriptor> getSlots() {
        return slots;
    }

    public SlotDescriptor getSlot(int slotId) {
        for (SlotDescriptor slot : slots) {
            if (slot.getId().asInt() == slotId) {
                return slot;
            }
        }
        return null;
    }

    public ArrayList<SlotDescriptor> getMaterializedSlots() {
        ArrayList<SlotDescriptor> result = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            if (slot.isMaterialized()) {
                result.add(slot);
            }
        }
        return result;
    }

    /**
     * Return slot descriptor corresponding to column referenced in the context
     * of tupleDesc, or null if no such reference exists.
     */
    public SlotDescriptor getColumnSlot(String columnName) {
        for (SlotDescriptor slotDesc : slots) {
            if (slotDesc.getColumn() != null && slotDesc.getColumn().getName().equalsIgnoreCase(columnName)) {
                return slotDesc;
            }
        }
        return null;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table tbl) {
        table = tbl;
    }

    public boolean getIsMaterialized() {
        return isMaterialized;
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    public void setIsMaterialized(boolean value) {
        isMaterialized = value;
    }

    public void setAliases(String[] aliases, boolean hasExplicitAlias) {
        aliases_ = aliases;
        hasExplicitAlias_ = hasExplicitAlias;
    }

    public boolean hasExplicitAlias() {
        return hasExplicitAlias_;
    }

    public String getAlias() {
        return (aliases_ != null) ? aliases_[0] : null;
    }

    public TTupleDescriptor toThrift() {
        TTupleDescriptor ttupleDesc = new TTupleDescriptor(id.asInt(), -1, -1);
        ttupleDesc.setNumNullSlots(-1);
        if (table != null && table.getId() >= 0) {
            ttupleDesc.setTableId((int) table.getId());
        }
        return ttupleDesc;
    }

    public void computeMemLayout() {
    }

    @Override
    public String toString() {
        String tblStr = (table == null ? "null" : table.getName());
        List<String> slotStrings = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            slotStrings.add(slot.debugString());
        }
        return MoreObjects.toStringHelper(this).add("id", id.asInt()).add("tbl", tblStr)
                .add("is_materialized", isMaterialized).add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
                .toString();
    }

    public String debugString() {
        String tblStr = (getTable() == null ? "null" : getTable().getName());
        List<String> slotStrings = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            slotStrings.add(slot.debugString());
        }
        return MoreObjects.toStringHelper(this)
                .add("id", id.asInt())
                .add("name", debugName)
                .add("tbl", tblStr)
                .add("is_materialized", isMaterialized)
                .add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
                .toString();
    }

}
