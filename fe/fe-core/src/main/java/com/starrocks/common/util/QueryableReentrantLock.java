// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

/*
 * This Lock is for exposing the getOwner() method,
 * which is a protected method of ReentrantLock
 */
public class QueryableReentrantLock extends FairReentrantLock {
    private static final long serialVersionUID = 1L;

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/common/util/QueryableReentrantLock.java
    public QueryableReentrantLock() {
        super();
    }

    public QueryableReentrantLock(boolean fair) {
        super(fair);
    }

=======
>>>>>>> 6d00614433 ([Enhancement] Use fair lock to avoid lock starvation (#44662)):fe/fe-core/src/main/java/com/starrocks/common/util/concurrent/QueryableReentrantLock.java
    @Override
    public Thread getOwner() {
        return super.getOwner();
    }
}
