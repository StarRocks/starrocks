/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starrocks.data.load.stream;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public interface StreamLoadDataFormat
{
    StreamLoadDataFormat JSON = new JSONFormat();
    StreamLoadDataFormat CSV = new CSVFormat();

    byte[] first();

    byte[] delimiter();

    byte[] end();

    class CSVFormat
            implements StreamLoadDataFormat, Serializable
    {
        private static final byte[] NEW_LINE = "\n".getBytes(StandardCharsets.UTF_8);
        private final byte[] delimiter;

        public CSVFormat()
        {
            this("\n");
        }

        public CSVFormat(String rowDelimiter)
        {
            if (rowDelimiter == null) {
                throw new IllegalArgumentException("row delimiter can not be null");
            }
            this.delimiter = rowDelimiter.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] first()
        {
            return NEW_LINE;
        }

        @Override
        public byte[] delimiter()
        {
            return delimiter;
        }

        @Override
        public byte[] end()
        {
            return NEW_LINE;
        }
    }

    class JSONFormat
            implements StreamLoadDataFormat, Serializable
    {
        private static final byte[] first = "[".getBytes(StandardCharsets.UTF_8);
        private static final byte[] delimiter = ",".getBytes(StandardCharsets.UTF_8);
        private static final byte[] end = "]".getBytes(StandardCharsets.UTF_8);

        @Override
        public byte[] first()
        {
            return first;
        }

        @Override
        public byte[] delimiter()
        {
            return delimiter;
        }

        @Override
        public byte[] end()
        {
            return end;
        }
    }
}
