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
        FEATURES = ImmutableList.copyOf(features);
    }

    // get all features
    public static List<ProductFeature> getFeatures() {
        return FEATURES;
    }
}
