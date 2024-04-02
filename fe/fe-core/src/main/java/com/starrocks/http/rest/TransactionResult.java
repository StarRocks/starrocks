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

package com.starrocks.http.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class TransactionResult extends RestBaseResult {

    public static final String STATUS_KEY = "Status";
    public static final String MESSAGE_KEY = "Message";

    public static final String TXN_ID_KEY = "TxnId";
    public static final String LABEL_KEY = "Label";

    private final Map<String, Object> resultMap;

    public TransactionResult() {
        status = ActionStatus.OK;
        msg = "";
        resultMap = new HashMap<>(0);
    }

    public void addResultEntry(String key, Object value) {
        resultMap.put(key, value);
    }

    public void setErrorMsg(String errMsg) {
        status = ActionStatus.FAILED;
        msg = errMsg;
    }

    public void setOKMsg(String okMsg) {
        msg = okMsg;
    }

    public boolean containMsg() {
        return StringUtils.isNotEmpty(msg);
    }

    public boolean stateOK() {
        return status == ActionStatus.OK;
    }

    public String toJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        addResultEntry(STATUS_KEY, status);
        addResultEntry(MESSAGE_KEY, msg);
        return gson.toJson(resultMap);
    }
}