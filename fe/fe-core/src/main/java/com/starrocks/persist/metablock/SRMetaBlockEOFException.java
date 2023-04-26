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


package com.starrocks.persist.metablock;

/**
 * This exception indicates a permitted EOF and should be captured and ignored.
 * A common scenario is when FE rollbacks after upgraded to a higher version and generated a image with more metadata
 */
public class SRMetaBlockEOFException extends Exception {

    public SRMetaBlockEOFException(String msg) {
        super(msg);
    }
}
