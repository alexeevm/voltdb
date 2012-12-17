/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.planner;

import java.util.List;

import junit.framework.TestCase;

import org.voltdb.plannodes.*;
import org.voltdb.types.PlanNodeType;

public class TestPlansJoin extends TestCase {
    private PlannerTestAideDeCamp aide;

    private AbstractPlanNode compile(String sql, int paramCount,
                                     boolean singlePartition,
                                     String joinOrder)
    {
        List<AbstractPlanNode> pn = null;
        try {
            pn =  aide.compile(sql, paramCount, singlePartition, joinOrder);
        }
        catch (NullPointerException ex) {
            // aide may throw NPE if no plangraph was created
            ex.printStackTrace();
            fail();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
        assertTrue(pn != null);
        assertFalse(pn.isEmpty());
        assertTrue(pn.get(0) != null);
        return pn.get(0);
    }

    public void testBasicJoin() {
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R2 ON R1.C = R2.C", 0, false, null);
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        for (int ii = 0; ii < 2; ii++) {
            assertTrue(n.getChild(ii) instanceof SeqScanPlanNode);
        }

        pn = compile("select C FROM R1 JOIN R2 ON R1.C = R2.C", 0, false, null);
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        pn = compile("select R1.C, R2.C FROM R1 JOIN R2 ON R1.C = R2.C", 0, false, null);
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);

        pn = compile("select * FROM R1 JOIN R2 USING (C)", 0, false, null);
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        for (int ii = 0; ii < 2; ii++) {
            assertTrue(n.getChild(ii) instanceof SeqScanPlanNode);
        }

        pn = compile("select C FROM R1 JOIN R2 USING (C)", 0, false, null);
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        pn = compile("select R1.C, R2.C FROM R1 JOIN R2 USING (C)", 0, false, null);
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
    }

   public void testIndexJoin() {
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R3 ON R1.C = R3.A", 0, false, null);
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getInlinePlanNode(PlanNodeType.INDEXSCAN) != null);

        pn = compile("select * FROM R3 JOIN R1 ON R1.C = R3.A", 0, false, null);
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getInlinePlanNode(PlanNodeType.INDEXSCAN) != null);
    }

   public void testDistributedJoin() {
       // JOIN replicated and one distributed table
       AbstractPlanNode pn = compile("select * FROM R1 JOIN P2 ON R1.C = P2.A", 0, false, null);
       AbstractPlanNode n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);

       // Join multiple distributed tables on the partitioned column
       pn = compile("select * FROM P1 JOIN P2 USING(A)", 0, false, null);
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);

       // Two Distributed tables join on non-partitioned column
       try {
           pn = compile("select * FROM P1 JOIN P2 ON P1.C = P2.A", 0, false, null);
       } catch (NullPointerException ex) {
           fail();
       }
   }

    @Override
    protected void setUp() throws Exception {
        aide = new PlannerTestAideDeCamp(TestJoinOrder.class.getResource("testplansjoin-ddl.sql"),
                                         "testplansjoin");

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        aide.tearDown();
    }
}
