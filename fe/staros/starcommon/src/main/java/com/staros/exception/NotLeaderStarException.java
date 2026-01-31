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

package com.staros.exception;

import com.staros.proto.LeaderInfo;
import com.staros.proto.StarStatus;
import com.staros.proto.StatusCode;

public class NotLeaderStarException extends StarException {
    private static final ExceptionCode CODE = ExceptionCode.NOT_LEADER;
    private final LeaderInfo leader;

    public NotLeaderStarException(String msg) {
        super(CODE, msg);
        this.leader = null;
    }

    public NotLeaderStarException(LeaderInfo leader, String msg) {
        super(CODE, msg);
        this.leader = leader;
    }

    public NotLeaderStarException(String messagePattern, Object ... params) {
        super(CODE, messagePattern, params);
        this.leader = null;
    }

    public LeaderInfo getLeaderInfo() {
        return leader;
    }

    @Override
    public StarStatus toStatus() {
        if (leader == null) {
            return super.toStatus();
        } else {
            return StarStatus.newBuilder()
                    .setStatusCode(StatusCode.NOT_LEADER)
                    .setExtraInfo(leader.toByteString())
                    .setErrorMsg(getMessage())
                    .build();
        }
    }
}
