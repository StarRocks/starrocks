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

package com.starrocks.format.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.StringJoiner;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionResult {

    @JsonProperty("Status")
    private String status;

    @JsonProperty("Message")
    private String message;

    @JsonProperty("Label")
    private String label;

    @JsonProperty("TxnId")
    private Long txnId;

    public TransactionResult() {
    }

    public boolean isOk() {
        return "OK".equalsIgnoreCase(getStatus());
    }

    public boolean notOk() {
        return !isOk();
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", "[", "]")
                .add("status='" + status + "'")
                .add("message='" + message + "'")
                .add("label='" + label + "'")
                .add("txnId=" + txnId)
                .toString();
    }

    public String getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public String getLabel() {
        return label;
    }

    public Long getTxnId() {
        return txnId;
    }

}
