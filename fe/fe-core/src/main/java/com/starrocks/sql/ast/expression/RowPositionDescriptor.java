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

import com.google.common.base.Preconditions;
import com.starrocks.planner.SlotId;
import com.starrocks.thrift.TRowPositionDescriptor;
import com.starrocks.thrift.TRowPositionType;

import java.util.List;

/**
 * Describes how to locate a specific row in a table for global late materialization.
 *
 * In global late materialization, scan operators initially output only computation-related columns
 * (e.g., predicate columns) and row position columns (instead of all columns) to minimize data movement.
 * Later, when additional columns are needed, FetchNode uses this descriptor to request LookUpNode
 * to materialize the specific rows.
 */
public class RowPositionDescriptor {
    public enum Type {
        ICEBERG_V3
    }

    // Type of row position format, determines how row positions are encoded and interpreted.
    private Type type;

    // slot id that identifies the BE/CN where the row originated.
    // it will determine which compute node FetchNode sends the request to
    private SlotId rowSourceSlot;

    // slot ids that contain row position information in FetchNode.
    // for Iceberg V3: [scan_range_id, row_id]
    private List<SlotId> fetchRefSlots;

    // slot ids in LookUpNode that correspond to fetchRefSlots.
    // used by LookUpNode to receive position info and materialize rows.
    // must have the same size as fetchRefSlots.
    private List<SlotId> lookupRefSlots;

    public RowPositionDescriptor(Type type, SlotId rowSourceSlot, List<SlotId> fetchRefSlots, List<SlotId> lookupRefSlots) {
        Preconditions.checkState(fetchRefSlots != null && !fetchRefSlots.isEmpty(),
                "fetchRefSlots can't be null or empty");
        Preconditions.checkState(lookupRefSlots != null && !lookupRefSlots.isEmpty(),
                "lookupRefSlots can't be null or empty");
        Preconditions.checkState(fetchRefSlots.size() == lookupRefSlots.size(),
                "fetchRefSlots'size shoule be same with lookupRefSlots");
        this.type = type;
        this.rowSourceSlot = rowSourceSlot;
        this.fetchRefSlots = fetchRefSlots;
        this.lookupRefSlots = lookupRefSlots;
    }

    public Type getType() {
        return type;
    }

    public SlotId getRowSourceSlot() {
        return rowSourceSlot;
    }

    public List<SlotId> getFetchRefSlots() {
        return fetchRefSlots;
    }

    public List<SlotId> getLookupRefSlots() {
        return lookupRefSlots;
    }


    public TRowPositionDescriptor toThrift() {
        TRowPositionDescriptor msg = new TRowPositionDescriptor();
        switch (type) {
            case ICEBERG_V3:
                msg.setRow_position_type(TRowPositionType.ICEBERG_V3_ROW_POSITION);
                break;
            default:
                throw new RuntimeException("unknown type");
        }
        msg.setRow_source_slot(rowSourceSlot.asInt());
        fetchRefSlots.forEach(slotId -> msg.addToFetch_ref_slots(slotId.asInt()));
        lookupRefSlots.forEach(slotId -> msg.addToLookup_ref_slots(slotId.asInt()));
        return msg;
    }
}
