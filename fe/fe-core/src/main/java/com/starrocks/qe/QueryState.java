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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/QueryState.java

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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.ErrorCode;
import com.starrocks.mysql.MysqlEofPacket;
import com.starrocks.mysql.MysqlErrPacket;
import com.starrocks.mysql.MysqlOkPacket;
import com.starrocks.mysql.MysqlPacket;

// query state used to record state of query, maybe query status is better
public class QueryState {
    public enum MysqlStateType {
        NOOP,   // send nothing to remote
        OK,     // send OK packet to remote
        EOF,    // send EOF packet to remote
        ERR;     // send ERROR packet to remote

        private static final ImmutableMap<String, MysqlStateType> STATES =
                new ImmutableMap.Builder<String, MysqlStateType>()
                        .put("NOOP", NOOP)
                        .put("OK", OK)
                        .put("EOF", EOF)
                        .put("ERR", ERR)
                        .build();

        public static MysqlStateType fromString(String state) {
            return STATES.get(state);
        }
    }

    public enum ErrType {
        ANALYSIS_ERR,
        OTHER_ERR
    }

    private MysqlStateType stateType = MysqlStateType.OK;
    private String errorMessage = "";
    private ErrorCode errorCode;
    private String infoMessage;
    private ErrType errType = ErrType.OTHER_ERR;
    private boolean isQuery = false;
    private long affectedRows = 0;
    private int warningRows = 0;
    // make it public for easy to use
    public int serverStatus = 0;
    private boolean isFinished = false;

    public QueryState() {
    }

    public void reset() {
        stateType = MysqlStateType.OK;
        errorCode = null;
        infoMessage = null;
        serverStatus = 0;
        isQuery = false;
        isFinished = false;
    }

    public MysqlStateType getStateType() {
        return stateType;
    }

    public void setEof() {
        stateType = MysqlStateType.EOF;
        isFinished = true;
    }

    public void setOk() {
        setOk(0, 0, null);
    }

    public void setOk(long affectedRows, int warningRows, String infoMessage) {
        this.affectedRows = affectedRows;
        this.warningRows = warningRows;
        this.infoMessage = infoMessage;
        stateType = MysqlStateType.OK;
        isFinished = true;
    }

    public void setError(String errorMsg) {
        this.stateType = MysqlStateType.ERR;
        this.setMsg(errorMsg);
        isFinished = true;
    }

    public boolean isError() {
        return stateType == MysqlStateType.ERR;
    }

    public boolean isRunning() {
        return !isFinished;
    }

    public void setStateType(MysqlStateType stateType) {
        this.stateType = stateType;
    }

    public void setMsg(String msg) {
        this.errorMessage = msg == null ? "" : msg;
    }

    public void setErrType(ErrType errType) {
        this.errType = errType;
    }

    public ErrType getErrType() {
        return errType;
    }

    public void setIsQuery(boolean isQuery) {
        this.isQuery = isQuery;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public int getWarningRows() {
        return warningRows;
    }

    public MysqlPacket toResponsePacket() {
        MysqlPacket packet = null;
        switch (stateType) {
            case OK:
                packet = new MysqlOkPacket(this);
                break;
            case EOF:
                packet = new MysqlEofPacket(this);
                break;
            case ERR:
                packet = new MysqlErrPacket(this);
                break;
            default:
                break;
        }
        return packet;
    }

    public String toProfileString() {
        if (stateType == MysqlStateType.ERR) {
            return "Error";
        } else if (isFinished) {
            return "Finished";
        } else {
            return "Running";
        }
    }

    @Override
    public String toString() {
        return String.valueOf(stateType);
    }
}
