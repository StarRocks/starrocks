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

package com.starrocks.alter.dynamictablet;

/*
 * DynamicTabletContext saves the context during tablet splitting or merging for a tablet
 */
public class DynamicTabletContext {

    protected final DynamicTablet dynamicTablet;

    // The physical partition version of tablet splitting or merging.
    // Versions greater than it need cross publish until dynamic tablet job finished.
    // This field is used to check which versions need cross publish.
    protected final long visibleVersion;

    public DynamicTabletContext(DynamicTablet dynamicTablet, long visibleVersion) {
        this.dynamicTablet = dynamicTablet;
        this.visibleVersion = visibleVersion;
    }

    public DynamicTablet getDynamicTablet() {
        return dynamicTablet;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }
}
