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


package com.starrocks.common;

public class CsvFormat {
    public CsvFormat(byte enclose, byte escape, long skipheader, boolean trimspace) {
        this.enclose = enclose;
        this.escape = escape;
        this.skipheader = skipheader;
        this.trimspace = trimspace;
    }
    public byte getEnclose() {
        return enclose;
    }

    public byte getEscape() {
        return escape;
    }

    public long getSkipheader() {
        return skipheader;
    }

    public boolean isTrimspace() {
        return trimspace;
    }

    private byte enclose;
    private byte escape;
    private long skipheader;
    private boolean trimspace;
}
