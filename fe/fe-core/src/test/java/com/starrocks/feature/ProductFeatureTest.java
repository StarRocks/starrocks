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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ProductFeatureTest {

    @Test
    public void testProductFeature() {
        List<ProductFeature> features = ProductFeature.getFeatures();
        Assertions.assertEquals(9, features.size());
        Assertions.assertEquals("multi-cngroup", features.get(4).getName());
        Assertions.assertEquals("automated-cluster-snapshot", features.get(6).getName());
        Assertions.assertEquals("oidc-email-username", features.get(7).getName());
        Assertions.assertEquals("cross-region-recovery", features.get(8).getName());
    }

    @Test
    public void testFeatureNames() {
        List<ProductFeature> features = ProductFeature.getFeatures();
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("RBAC")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("multi-warehouse")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("warehouse-query-queue")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("license")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("multi-cngroup")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("ArrowFlightSQL")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("automated-cluster-snapshot")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("oidc-email-username")));
        Assertions.assertTrue(features.stream().anyMatch(feature -> feature.getName().equals("cross-region-recovery")));
    }

    @Test
    public void testCrossRegionRecoveryFeature() {
        ProductFeature feature = ProductFeature.getFeatures().stream()
                .filter(f -> f.getName().equals("cross-region-recovery"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("cross-region-recovery feature not found"));
        Assertions.assertTrue(feature.getDescription().contains("cluster snapshot"));
        Assertions.assertTrue(feature.getDescription().contains("shared-data"));
        Assertions.assertEquals("https://docs.starrocks.io/docs/administration/cluster_snapshot/", feature.getLink());
    }

    @Test
    public void testOidcEmailUsernameFeature() {
        ProductFeature feature = ProductFeature.getFeatures().stream()
                .filter(f -> f.getName().equals("oidc-email-username"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("oidc-email-username feature not found"));
        Assertions.assertEquals("", feature.getLink());
        Assertions.assertTrue(feature.getDescription().contains("authentication_oauth2"));
        Assertions.assertTrue(feature.getDescription().contains("authentication_jwt"));
    }
}
