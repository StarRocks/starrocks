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

package com.starrocks.memory.estimate;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

/**
 * Custom estimator for heap-based ByteBuffer instances.
 */
public class ByteBufferEstimator implements CustomEstimator {
    @Override
    public long estimate(Object obj) {
        Preconditions.checkArgument(obj instanceof ByteBuffer);
        ByteBuffer buffer = (ByteBuffer) obj;
        long size = Estimator.shallow(buffer);

        if (buffer.isDirect()) {
            // Direct buffers are off-heap; only count the wrapper itself.
            return size;
        }

        try {
            if (buffer.hasArray()) {
                byte[] array = buffer.array();
                return size + Estimator.ARRAY_HEADER_SIZE + array.length;
            }
        } catch (UnsupportedOperationException ignore) {
            // fall through to shallow size
        }

        return size;
    }
}
