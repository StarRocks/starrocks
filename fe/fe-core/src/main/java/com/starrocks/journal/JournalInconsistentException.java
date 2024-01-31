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


package com.starrocks.journal;

import com.starrocks.persist.OperationType;

/**
 * if inconsistent issue occurs when handling journal, this exception is called, indicating it's time to quit the process
 */
public class JournalInconsistentException extends Exception {
    private short opCode = OperationType.OP_INVALID;

    public JournalInconsistentException(String errMsg)  {
        super(errMsg);
    }

    public JournalInconsistentException(short opCode, String errMsg)  {
        super(errMsg);
        this.opCode = opCode;
    }

    public short getOpCode() {
        return opCode;
    }
}
