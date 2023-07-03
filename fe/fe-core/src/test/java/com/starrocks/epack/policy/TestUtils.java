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
package com.starrocks.epack.policy;

import org.junit.function.ThrowingRunnable;

public class TestUtils {
    public static <T extends Throwable> T assertThrows(String expectExceptionMessage,
                                                       Class<T> expectedThrowable, ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable actualThrown) {
            if (expectedThrowable.isInstance(actualThrown)) {
                @SuppressWarnings("unchecked") T retVal = (T) actualThrown;
                if (!actualThrown.getMessage().contains(expectExceptionMessage)) {
                    AssertionError assertionError = new AssertionError("exception message error");
                    assertionError.initCause(actualThrown);
                    throw assertionError;
                }
                return retVal;
            }
            AssertionError assertionError = new AssertionError("unexpected exception type thrown");
            assertionError.initCause(actualThrown);
            throw assertionError;
        }

        throw new AssertionError("expected %s to be thrown, but nothing was thrown");
    }
}
