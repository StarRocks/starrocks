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

package com.starrocks.transaction;

public class TransactionCommitFailedException extends TransactionException {

    private static final long serialVersionUID = -2528170792631761535L;

    public static final String NO_DATA_TO_LOAD_MSG = "all partitions have no load data";
    public static final String FILTER_DATA_IN_STRICT_MODE = "filter data in strict mode";

    public TransactionCommitFailedException(String msg) {
        super(msg);
    }

    public TransactionCommitFailedException(String msg, Throwable e) {
        super(msg, e);
    }
}
