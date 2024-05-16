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
package com.starrocks.common.util.concurrent.lock;


public class DeadlockException extends IllegalLockStateException {
    public DeadlockException(String msg) {
        super(msg);
    }

    public static DeadlockException makeDeadlockException(
            final LockManager.DeadLockChecker dc, final Locker locker, final boolean isVictim) {

        StringBuilder msg = new StringBuilder();
        msg.append("Deadlock was detected. ");
        if (isVictim) {
            msg.append("Locker: \"").append(locker);
            msg.append("\" was chosen randomly as the victim.\n");
        } else {
            msg.append("Unable to break deadlock using random victim ");
            msg.append("selection within the timeout interval. ");
            msg.append("Current locker: \"").append(locker);
            msg.append("\" must be aborted.\n");
        }

        msg.append(dc);

        return new DeadlockException(msg.toString());
    }
}
