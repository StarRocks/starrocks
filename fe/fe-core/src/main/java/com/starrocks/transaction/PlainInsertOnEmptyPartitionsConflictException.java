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

package com.starrocks.transaction;

/**
 * A column-subset INSERT was planned as a plain insert because no transaction had ever committed to
 * its target partitions, and by the time it committed another transaction had. Its filled defaults
 * would overwrite that transaction's columns, so the commit is refused. The statement is safe to run
 * again: re-planned, it sees the advanced partition version and runs as a partial update.
 */
public class PlainInsertOnEmptyPartitionsConflictException extends TransactionException {

    private static final long serialVersionUID = 1L;

    public PlainInsertOnEmptyPartitionsConflictException(String msg) {
        super(msg);
    }
}
