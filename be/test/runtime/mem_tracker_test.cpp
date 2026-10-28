#include "runtime/mem_tracker.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(MemTrackerTest, ReplicationAccountingExcludesProcessRoot) {
    MemTracker process(-1, "process");
    MemTracker load(100, "load", &process);
    MemTracker replicate(-1, "replicate", &load);

    process.consume(80);
    replicate.consume_without_root(80);
    EXPECT_EQ(80, process.consumption());
    EXPECT_EQ(80, load.consumption());
    EXPECT_EQ(80, replicate.consumption());
    EXPECT_FALSE(load.limit_exceeded());

    replicate.release_without_root(80);
    EXPECT_EQ(80, process.consumption());
    EXPECT_EQ(0, load.consumption());
    EXPECT_EQ(0, replicate.consumption());

    process.release(80);
}

TEST(MemTrackerTest, ReplicationCancellationClearsLoadAccounting) {
    MemTracker process(-1, "process");
    MemTracker load(100, "load", &process);

    process.consume(120);
    {
        MemTracker replicate(-1, "replicate", &load);
        replicate.consume_without_root(120);
        EXPECT_TRUE(replicate.parent()->limit_exceeded());
        EXPECT_EQ(120, load.consumption());
        EXPECT_EQ(120, process.consumption());
    }
    EXPECT_EQ(0, load.consumption());
    EXPECT_EQ(120, process.consumption());
    process.release(120);
}

}
