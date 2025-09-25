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


package com.staros.util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Take cares of start()/stop() interface with Atomic variables to avoid repeatedly start/stop.
 *   User only needs to implement its doStart()/doStop() and check if needs to quit loop by isRunning().
 */
public abstract class AbstractServer implements Server {
    protected AtomicBoolean running = new AtomicBoolean(false);

    public abstract void doStart();
    public abstract void doStop();

    @Override
    public void start() {
        if (!running.compareAndSet(false /* expect */, true /* wanted */)) {
            return;
        }
        doStart();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true /* expect */, false /* wanted */)) {
            return;
        }
        doStop();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }
}
