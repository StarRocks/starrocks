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

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

public class TransactionResult extends RestBaseResult {
    private Map<String, Object> resultMap = Maps.newHashMap();

    public TransactionResult() {
        status = ActionStatus.OK;
        msg = "";
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
        return msg.length() > 0;
    }

    public boolean stateOK() {
        return status == ActionStatus.OK;
    }
    

    public String toJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        addResultEntry("Status", status);
        addResultEntry("Message", msg);
        return gson.toJson(resultMap);
    }
}