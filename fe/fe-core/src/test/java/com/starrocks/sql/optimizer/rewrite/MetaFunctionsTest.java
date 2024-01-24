package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Type;
import com.starrocks.leader.ReportHandler;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

public class MetaFunctionsTest {

    @Test
    public void testInspectMemory() {
        MemoryUsageTracker.registerMemoryTracker("Report", new ReportHandler());
        try {
            MetaFunctions.inspectMemory(new ConstantOperator("abc", Type.VARCHAR));
            Assert.fail();
        } catch (Exception ex) {
        }
        MetaFunctions.inspectMemory(new ConstantOperator("report", Type.VARCHAR));
    }

    @Test
    public void testInspectMemoryDetail() {
        MemoryUsageTracker.registerMemoryTracker("Report", new ReportHandler());
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("abc", Type.VARCHAR),
                    new ConstantOperator("def", Type.VARCHAR));
            Assert.fail();
        } catch (Exception ex) {
        }
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("report", Type.VARCHAR),
                    new ConstantOperator("def", Type.VARCHAR));
            Assert.fail();
        } catch (Exception ex) {
        }
        try {
            MetaFunctions.inspectMemoryDetail(
                    new ConstantOperator("report", Type.VARCHAR),
                    new ConstantOperator("reportHandler.abc", Type.VARCHAR));
            Assert.fail();
        } catch (Exception ex) {
        }
        MetaFunctions.inspectMemoryDetail(
                new ConstantOperator("report", Type.VARCHAR),
                new ConstantOperator("reportHandler", Type.VARCHAR));
        MetaFunctions.inspectMemoryDetail(
                new ConstantOperator("report", Type.VARCHAR),
                new ConstantOperator("reportHandler.reportQueue", Type.VARCHAR));
    }
}
