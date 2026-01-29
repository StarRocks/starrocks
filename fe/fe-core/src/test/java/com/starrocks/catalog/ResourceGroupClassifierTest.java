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

package com.starrocks.catalog;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class ResourceGroupClassifierTest {
    @Test
    public void testSourceIP() {
        ResourceGroupClassifier classifier = new ResourceGroupClassifier();

        classifier.setSourceIp("192.168.0.1/32");
        assertThat(classifier.isVisible("user", null, "192.168.0.1")).isTrue();
        assertThat(classifier.isVisible("user", null, "192.168.0.2")).isFalse();

        classifier.setSourceIp("192.168.0.1/31");
        assertThat(classifier.isVisible("user", null, "192.168.0.1")).isTrue();
        assertThat(classifier.isVisible("user", null, "192.168.0.2")).isFalse();

        classifier.setSourceIp("192.168.0.1/30");
        assertThat(classifier.isVisible("user", null, "192.168.0.1")).isTrue();
        assertThat(classifier.isVisible("user", null, "192.168.0.2")).isTrue();
        assertThat(classifier.isVisible("user", null, "192.168.1.1")).isFalse();
    }

    @Test
    public void testWeight() {
        ResourceGroupClassifier classifier = new ResourceGroupClassifier();

        for (int i = 0; i <= 32; i++) {
            classifier.setSourceIp("192.168.0.1/" + i);
            assertThat(classifier.weight()).isCloseTo(1 + i / 64., within(1e-5));
        }
    }

    @Test
    public void testUserPattern() {
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user1").matches()).isTrue();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user_1").matches()).isTrue();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user-1").matches()).isTrue();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("a-user1").matches()).isTrue();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user1/host").matches()).isTrue();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user1/host-1").matches()).isTrue();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("").matches()).isFalse();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("-user1").matches()).isFalse();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user@").matches()).isFalse();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user/host/extra").matches()).isFalse();
        assertThat(ResourceGroupClassifier.USER_PATTERN.matcher("user with space").matches()).isFalse();
    }
}
