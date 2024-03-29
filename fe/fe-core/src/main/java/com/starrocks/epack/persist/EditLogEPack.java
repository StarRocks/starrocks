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
package com.starrocks.epack.persist;

import com.starrocks.epack.server.WarehouseManagerEPack;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.Warehouse;

import java.util.concurrent.BlockingQueue;

public class EditLogEPack extends EditLog {
    public EditLogEPack(BlockingQueue<JournalTask> journalQueue) {
        super(journalQueue);
    }

    // warehouse
    public void logCreateWarehouse(Warehouse warehouse) {
        logEdit(OperationTypeEPack.OP_CREATE_WAREHOUSE, warehouse);
    }

    public void logDropWarehouse(DropWarehouseLog log) {
        logEdit(OperationTypeEPack.OP_DROP_WAREHOUSE, log);
    }

    public void logAlterWarehouse(Warehouse wh) {
        logEdit(OperationTypeEPack.OP_ALTER_WAREHOUSE, wh);
    }

    @Override
    public void loadJournal(GlobalStateMgr globalStateMgr, JournalEntity journal)
            throws JournalInconsistentException {

        short opCode = journal.getOpCode();
        try {
            switch (opCode) {
                case OperationTypeEPack.OP_CREATE_WAREHOUSE: {
                    Warehouse wh = (Warehouse) journal.getData();
                    WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) globalStateMgr.getWarehouseMgr();
                    warehouseMgr.replayCreateWarehouse(wh);
                    break;
                }
                case OperationTypeEPack.OP_DROP_WAREHOUSE: {
                    DropWarehouseLog log = (DropWarehouseLog) journal.getData();
                    WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) globalStateMgr.getWarehouseMgr();
                    warehouseMgr.replayDropWarehouse(log);
                    break;
                }
                case OperationTypeEPack.OP_ALTER_WAREHOUSE: {
                    Warehouse wh = (Warehouse) journal.getData();
                    WarehouseManagerEPack warehouseMgr = (WarehouseManagerEPack) globalStateMgr.getWarehouseMgr();
                    warehouseMgr.replayAlterWarehouse(wh);
                    break;
                }
                default: {
                    super.loadJournal(globalStateMgr, journal);
                }
            }
        } catch (Exception e) {
            JournalInconsistentException exception =
                    new JournalInconsistentException(opCode, "failed to load journal type " + opCode);
            exception.initCause(e);
            throw exception;
        }
    }
}
