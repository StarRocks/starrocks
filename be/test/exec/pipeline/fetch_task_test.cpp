#include "exec/pipeline/fetch_task.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"

namespace starrocks::pipeline {

TEST(FetchTaskTest, batch_unit_cycle_is_released_after_outer_refs_drop) {
    std::weak_ptr<BatchUnit> weak_unit;

    {
        auto unit = std::make_shared<BatchUnit>();
        weak_unit = unit;

        auto ctx = std::make_shared<FetchTaskContext>();
        ctx->unit = unit;

        auto task = std::make_shared<FetchTask>(ctx);
        auto tasks = std::make_shared<std::vector<FetchTaskPtr>>();
        tasks->emplace_back(task);
        unit->fetch_tasks.emplace(1, tasks);

        task.reset();
        tasks.reset();
        ctx.reset();
        unit.reset();
    }

    EXPECT_TRUE(weak_unit.expired());
}

} // namespace starrocks::pipeline
