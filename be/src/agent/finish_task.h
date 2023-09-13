#pragma once

namespace starrocks {

class TFinishTaskRequest;

void finish_task(const TFinishTaskRequest& finish_task_request);
} // namespace starrocks
