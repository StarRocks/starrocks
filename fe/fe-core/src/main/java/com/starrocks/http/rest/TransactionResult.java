// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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

    public String toJson() {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        addResultEntry("Status", status);
        addResultEntry("Message", msg);
        return gson.toJson(resultMap);
    }
}