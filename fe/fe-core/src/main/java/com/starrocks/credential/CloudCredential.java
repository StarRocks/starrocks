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

package com.starrocks.credential;

import com.starrocks.thrift.TCloudProperty;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public interface CloudCredential {

    /**
    * Set credentials into Hadoop configuration
    */
    void applyToConfiguration(Configuration configuration);

    /**
     * Check CloudCredential is valid.
     * We only check whether the credential format is legal,
     * and do not check whether its value is correct!
     */
    boolean validate();

    /**
     * Write credential into thrift.
     */
    void toThrift(List<TCloudProperty> properties);
}
