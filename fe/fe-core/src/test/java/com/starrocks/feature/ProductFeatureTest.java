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

package com.starrocks.feature;

import com.starrocks.common.feature.ProductFeature;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ProductFeatureTest {

    @Test
    public void testProductFeature() {
        List<ProductFeature> features = ProductFeature.getFeatures();
        Assert.assertEquals(1, features.size());
    }
}
