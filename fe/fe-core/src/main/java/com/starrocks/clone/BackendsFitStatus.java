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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/BalanceStatus.java

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

package com.starrocks.clone;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Status represents that whether we can find a backend path to clone new replica.
 */
public class BackendsFitStatus {
    public enum ErrCode {
        OK,
        COMMON_ERROR
    }

    private ErrCode errCode;
    private List<String> errMsgs = Lists.newArrayList();

    public static final BackendsFitStatus OK = new BackendsFitStatus(ErrCode.OK, "");

    public BackendsFitStatus(ErrCode errCode) {
        this.errCode = errCode;
    }

    public BackendsFitStatus(ErrCode errCode, String errMsg) {
        this.errCode = errCode;
        this.errMsgs.add(errMsg);
    }

    public ErrCode getErrCode() {
        return errCode;
    }

    public List<String> getErrMsgs() {
        return errMsgs;
    }

    public void addErrMsgs(List<String> errMsgs) {
        this.errMsgs.addAll(errMsgs);
    }

    public void addErrMsg(String errMsg) {
        this.errMsgs.add(errMsg);
    }

    public boolean ok() {
        return errCode == ErrCode.OK;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(errCode.name());
        if (!ok()) {
            sb.append(", msg: ").append(errMsgs);
        }
        sb.append("]");
        return sb.toString();
    }
}
