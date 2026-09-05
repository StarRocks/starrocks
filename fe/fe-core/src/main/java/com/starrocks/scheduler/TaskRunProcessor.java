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


package com.starrocks.scheduler;

public interface TaskRunProcessor {
    /**
     * Before task run, prepare the context.
     * @param context: task run context
     */
    TaskRunContext prepare(TaskRunContext context) throws Exception;

    /**
    * Process a task run with the given context.
    * @param context: the context containing information about the task run, such as the task definition,
    * @return: the state of the task runs after processing, which can be SUCCESS, FAILED, or SKIPPED.
    * @throws Exception: if any error occurs during the processing of the task run.
    */
    Constants.TaskRunState processTaskRun(TaskRunContext context) throws Exception;

    /**
     * Post-process after the task run is completed.
     * @param context: the context containing information about the task run, such as the task definition,
     * @throws Exception: if any error occurs during the post-processing of the task run.
     */
    void postTaskRun(TaskRunContext context) throws Exception;
}
