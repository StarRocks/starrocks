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


package com.staros.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Text {
    public static byte[] readBytes(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        return bytes;
    }

    public static void writeBytes(DataOutput out, byte[] bytes) throws IOException {
        int length = bytes.length;
        out.writeInt(length);
        out.write(bytes, 0, length);
    }
}
