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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

public class ProductFeature {
    private String name;
    private String version;
    private String description;
    private String link;

    private static final List<ProductFeature> FEATURES;

    public ProductFeature(String name, String description, String link) {
        this.name = name;
        this.description = description;
        this.link = link;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }

    public String getDescription() {
        return description;
    }

    public String getLink() {
        return link;
    }

    static {
        List<ProductFeature> features = new ArrayList<>();
        // add features here
        features.add(new ProductFeature(
                "RBAC",
                "privilege system with full RBAC functionalities, supporting role inheritance and default roles.",
                "https://docs.starrocks.io/en-us/latest/administration/privilege_overview"
        ));
        features.add(new ProductFeature(
                "multi-warehouse",
                "StarRocks supports creating multiple warehouses within a single cluster, with each warehouse " +
                        "using different computing resources. It supports assigning import, query and other tasks to a " +
                        "designated warehouse.",
                ""
        ));
        features.add(new ProductFeature(
                "warehouse-query-queue",
                "The warehouse-level query queue manages query execution by dynamically queuing or " +
                        "scaling compute resources based on real-time workload, ensuring performance while optimizing costs.",
                ""
        ));
        features.add(new ProductFeature("license", "license limitation", ""));
        features.add(new ProductFeature(
                "multi-cngroup",
                "Support creating multiple CN groups under the same warehouse.",
                ""
        ));
        features.add(new ProductFeature(
                "ArrowFlightSQL",
                "high-performance columnar data transfer using Apache Arrow.",
                "https://docs.starrocks.io/docs/unloading/arrow_flight/"
        ));
        features.add(new ProductFeature(
                "automated-cluster-snapshot",
                "Automatically creates consistent cluster snapshots at configured intervals for recovery and cloning.",
                "https://docs.starrocks.io/docs/administration/cluster_snapshot/"
        ));
        features.add(new ProductFeature(
                "oidc-email-username",
                "Allow email-format usernames for users created with authentication_oauth2 or " +
                        "authentication_jwt, so the StarRocks username can match an IdP principal " +
                        "claim (e.g. email / preferred_username) directly.",
                ""
        ));
        features.add(new ProductFeature(
                "cross-region-recovery",
                "Create cluster snapshots to external storage and bootstrap a new shared-data " +
                        "cluster from them via a startup YAML config, enabling cross-region disaster " +
                        "recovery and cluster cloning.",
                "https://docs.starrocks.io/docs/administration/cluster_snapshot/"
        ));
        FEATURES = ImmutableList.copyOf(features);
    }

    // get all features
    public static List<ProductFeature> getFeatures() {
        return FEATURES;
    }
}
