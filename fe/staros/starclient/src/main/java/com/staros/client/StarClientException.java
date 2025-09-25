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

package com.staros.client;

import com.staros.proto.StarStatus;
import com.staros.proto.StatusCode;

public class StarClientException extends Exception {
    private final StatusCode code;
    private byte[] extraInfo;

    public StarClientException(StatusCode code, String msg) {
        super(code.toString() + ":" + msg);
        this.code = code;
        this.extraInfo = new byte[0];
    }

    public StarClientException(StarStatus status) {
        this(status.getStatusCode(), status.getErrorMsg());
        this.extraInfo = status.getExtraInfo().toByteArray();
    }

    public StatusCode getCode() {
        return code;
    }

    public byte[] getExtraInfo() {
        return extraInfo;
    }
}
