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
package com.starrocks.alter;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

import java.util.List;

/**
 * used for batch log AlterJob to editLog in one atomic operation
 */
public class BatchAlterJobPersistInfo implements Writable {

    @SerializedName("jobs")
    private List<AlterJobV2> alterJobV2List;

    public BatchAlterJobPersistInfo(List<AlterJobV2> alterJobV2List) {
        this.alterJobV2List = alterJobV2List;
    }




    public List<AlterJobV2> getAlterJobV2List() {
        return alterJobV2List;
    }
}
