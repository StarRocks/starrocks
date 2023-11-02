#pragma once

namespace starrocks {

class TFinishTaskRequest;
class TFinishRequest;

void finish_task(const TFinishTaskRequest& finish_task_request);
void finish_req(const TFinishRequest& finish_request);
} // namespace starrocks
