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

package com.starrocks.alter.reshard;

import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;

/** Immutable admission identity for the stable-leader lifetime of a reshard job. */
final class ReshardAdmission {
    private final GlobalStateMgr owner;
    private final TabletReshardJobMgr manager;
    private final long epoch;

    private ReshardAdmission(GlobalStateMgr owner, TabletReshardJobMgr manager, long epoch) {
        this.owner = owner;
        this.manager = manager;
        this.epoch = epoch;
    }

    static ReshardAdmission capture(GlobalStateMgr owner, TabletReshardJobMgr manager) throws StarRocksException {
        try {
            ReshardAdmission admission = new ReshardAdmission(owner, manager, owner.getEpoch());
            if (admission.isValid()) {
                return admission;
            }
        } catch (RuntimeException e) {
            throw new StarRocksException("Unable to capture reshard leader admission", e);
        }
        throw new StarRocksException("Reshard admission requires a stable leader and positive epoch");
    }

    boolean isValid() {
        try {
            return epoch > 0 && owner == GlobalStateMgr.getCurrentState()
                    && manager == owner.getTabletReshardJobMgr()
                    && owner.isLeader() && !owner.isInTransferringToLeader()
                    && epoch == owner.getEpoch();
        } catch (RuntimeException e) {
            return false;
        }
    }

    void checkValid() throws StarRocksException {
        if (!isValid()) {
            throw new StarRocksException("Reshard admission leader changed");
        }
    }
}
