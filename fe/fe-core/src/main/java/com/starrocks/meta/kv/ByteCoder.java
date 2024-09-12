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

package com.starrocks.meta.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ByteCoder {
    public static byte[] encode(List<Object> paramList) {
        return encode(paramList.toArray());
    }

    public static byte[] encode(Object... paramList) {
        int capacity = 0;
        for (Object param : paramList) {
            if (param instanceof Long || param instanceof Integer) {
                // type byte
                capacity += 1;
                // long capacity 8
                capacity += 8;
            } else if (param instanceof String) {
                String s = (String) param;
                // type byte
                capacity += 1;
                //segment size
                int segmentSize = s.length() / 8 + 1;

                // 8 byte string and 1 byte valid byte
                capacity += segmentSize * 9;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(capacity);

        for (Object param : paramList) {
            if (param instanceof Integer) {
                buffer.put((byte) 0);
                buffer.putLong((Integer) param);
            } else if (param instanceof Long) {
                buffer.put((byte) 0);
                buffer.putLong((Long) param);
            } else if (param instanceof String) {
                String s = (String) param;
                int length = s.length();
                int segmentSize = length / 8;

                buffer.put((byte) 1);
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                for (int i = 0; i < segmentSize; ++i) {
                    buffer.put(bytes, i * 8, 8);
                    buffer.put((byte) 9);
                }

                buffer.put(bytes, 8 * segmentSize, length - 8 * segmentSize);
                buffer.position(buffer.position() + (segmentSize + 1) * 8 - length);
                buffer.put((byte) (length - 8 * segmentSize));
            }
        }

        return buffer.array();
    }

    public static List<Object> decode(byte[] params) {
        List<Object> result = new ArrayList<>();
        int ipos = 0;
        do {
            byte typeFlag = params[ipos];
            if ((int) typeFlag == 0) {
                ByteBuffer buffer = ByteBuffer.wrap(params, ipos + 1, 8);
                result.add(buffer.getLong());

                ipos = ipos + 9;
            } else if ((int) typeFlag == 1) {
                ipos = ipos + 1;

                int segmentSize;
                int strLength = 0;
                for (segmentSize = 0; ; ++segmentSize) {
                    int validLength = params[ipos + segmentSize * 9 + 8];
                    if (validLength < 9) {
                        strLength += validLength;
                        break;
                    } else {
                        strLength += 8;
                    }
                }

                byte[] buffer = new byte[strLength];
                for (int i = 0; i < segmentSize; ++i) {
                    System.arraycopy(params, ipos, buffer, i * 8, 8);
                    ipos = ipos + 9;
                }
                System.arraycopy(params, ipos, buffer, segmentSize * 8, strLength % 8);
                ipos = ipos + 9;

                result.add(new String(buffer, StandardCharsets.UTF_8));
            }

        } while (ipos < params.length);

        return result;
    }
}
