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

package com.staros.stream;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class NullInputStreamTest {

    @Test
    public void testAlwaysNoByte() throws IOException {
        try (InputStream in = new NullInputStream()) {
            Assert.assertEquals(-1, in.read());
        }

        try (InputStream in = new NullInputStream()) {
            byte[] data = new byte[1];
            Assert.assertEquals(-1, in.read(data));
        }

        try (InputStream in = new NullInputStream()) {
            byte[] data = new byte[1];
            Assert.assertEquals(-1, in.read(data, 0, 1));
        }
    }
}
