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

package com.starrocks.common;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.OlapTable;

import java.util.Objects;

public class InvalidOlapTableStateException extends DdlException {

    private InvalidOlapTableStateException(String message) {
        super(message);
    }

    public static InvalidOlapTableStateException of(OlapTable.OlapTableState state, String table) {
        Objects.requireNonNull(state, "state is null");
        Objects.requireNonNull(table, "table is null");
        Preconditions.checkState(state != OlapTable.OlapTableState.NORMAL);
        String errorPrompt;
        switch (state) {
            case ROLLUP:
                errorPrompt = String.format("A materialized view or rollup index is being created on the table \"%s\". " +
                                "Please wait until the current operation completes.\n" +
                                "To check the status of the operation, see %s or %s",
                        table,
                        FeConstants.DOCUMENT_SHOW_ALTER_MATERIALIZED_VIEW,
                        FeConstants.DOCUMENT_SHOW_ALTER);
                break;
            case SCHEMA_CHANGE:
                errorPrompt = String.format("A schema change operation is in progress on the table \"%s\". " +
                                "Please wait until the current operation completes.\n" +
                                "To check the status of the operation, see %s",
                        table,
                        FeConstants.DOCUMENT_SHOW_ALTER);
                break;
            case BACKUP:
                errorPrompt = String.format("A backup operation is in progress on the table \"%s\". " +
                                "Please wait until the current operation completes.\n" +
                                "To check the status of the operation, see %s",
                        table,
                        FeConstants.DOCUMENT_SHOW_BACKUP);
                break;
            case RESTORE:
            case RESTORE_WITH_LOAD:
                errorPrompt = String.format("A snapshot restore operation is in progress on the table \"%s\". " +
                                "Please wait until the current operation completes.\n" +
                                "To check the status of the operation, see %s",
                        table,
                        FeConstants.DOCUMENT_SHOW_RESTORE);
                break;
            case WAITING_STABLE:
                errorPrompt = "The table is being altered or having a materialized view built on it. " +
                        "Please wait until the current operation completes. \n" +
                        "To check the status of the operation, see \n" +
                        FeConstants.DOCUMENT_SHOW_ALTER + " or\n" + FeConstants.DOCUMENT_SHOW_ALTER_MATERIALIZED_VIEW;

                errorPrompt = String.format("The table \"%s\" is being altered or having a materialized view built on it. " +
                                "Please wait until the current operation completes.\n" +
                                "To check the status of the operation, see %s or %s",
                        table,
                        FeConstants.DOCUMENT_SHOW_ALTER,
                        FeConstants.DOCUMENT_SHOW_ALTER_MATERIALIZED_VIEW);
                break;
            default:
                errorPrompt = String.format("The table \"%s\" is currently in state \"%s\"", table, state.name());
                break;
        }

        return new InvalidOlapTableStateException(errorPrompt);
    }
}
