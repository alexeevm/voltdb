/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannerv2.rules.physical;

import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalExchange;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.utils.VoltRelUtil;

/**
 * Rules that to push Limit, Sort and Aggregation nodes down through an Exchange node
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPExchangeTransposeRule extends RelOptRule {

    private enum ExchangeType {
        LIMIT_EXCHANGE
    }

    /**
     * Predicate to stop LIMIT_EXCHANGE to match on LIMIT / EXCHANGE / LIMIT
    */
    private static final Predicate<VoltPhysicalLimit> LIMIT_STOP_PREDICATE = rel -> !rel.isPushedDown();
    public static final VoltPExchangeTransposeRule INSTANCE_LIMIT_EXCHANGE =
            new VoltPExchangeTransposeRule(
                    operandJ(VoltPhysicalLimit.class, null, LIMIT_STOP_PREDICATE,
                            operand(VoltPhysicalExchange.class,
                                    operand(RelNode.class, any()))),
                    ExchangeType.LIMIT_EXCHANGE);

    private final ExchangeType mExchangeType;

    private VoltPExchangeTransposeRule(RelOptRuleOperand operand, ExchangeType exchangeType) {
        super(operand, exchangeType.toString());
        mExchangeType = exchangeType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        switch (mExchangeType) {
            case LIMIT_EXCHANGE:
                transposeLimitExchange(call);
                break;
        }
    }

    private void transposeLimitExchange(RelOptRuleCall call) {
        VoltPhysicalLimit limit = (VoltPhysicalLimit) call.rels[0];
        Exchange exchange = (Exchange) call.rels[1];
        RelNode child = call.rels[2];

        VoltRelUtil.pushLimitThroughExchange(limit, exchange, child)
            .ifPresent(newLimit -> call.transformTo(newLimit));
    }

}
