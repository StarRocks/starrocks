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
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Dictionary.DictionaryState;
import com.starrocks.common.io.Writable;

public class UpdateDictionaryLog implements Writable {
    @SerializedName("dictionaryId")
    private final long dictionaryId;

    @SerializedName("ts")
    private final long ts;

    @SerializedName("state")
    private DictionaryState state;

    @SerializedName("lastSuccessVersion")
    private Long lastSuccessVersion;

    @SerializedName("errorMsg")
    private String errorMsg;

    @SerializedName("resetStateBeforeRefresh")
    private boolean resetStateBeforeRefresh;

    public UpdateDictionaryLog(long dictionaryId, long ts) {
        this.dictionaryId = dictionaryId;
        this.ts = ts;
    }

    public long getDictionaryId() {
        return dictionaryId;
    }

    public long getTs() {
        return ts;
    }

    public DictionaryState getState() {
        return state;
    }

    public void setState(DictionaryState state) {
        this.state = state;
    }

    public Long getLastSuccessVersion() {
        return lastSuccessVersion;
    }

    public void setLastSuccessVersion(Long lastSuccessVersion) {
        this.lastSuccessVersion = lastSuccessVersion;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void setResetStateBeforeRefresh(boolean flag) {
        this.resetStateBeforeRefresh = flag;
    }

    public boolean isResetStateBeforeRefresh() {
        return resetStateBeforeRefresh;
    }
}
