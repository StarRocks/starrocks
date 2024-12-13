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

package com.starrocks.qe.scheduler.dag;

<<<<<<< HEAD
import com.starrocks.common.UserException;
=======
import com.starrocks.common.StarRocksException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.rpc.RpcException;

import java.util.Collection;

public interface ScheduleNextTurnRunner {
<<<<<<< HEAD
    Collection<FragmentInstanceExecState> doSchedule() throws RpcException, UserException;
=======
    Collection<FragmentInstanceExecState> doSchedule() throws RpcException, StarRocksException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
