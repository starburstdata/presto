/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode.ExplainPlanNodeStatsAndCost;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

public class InsertStatsIntoExplainAnalyzeNode
        implements Rule<ExplainAnalyzeNode>
{
    private static final Pattern<ExplainAnalyzeNode> PATTERN = Pattern.typeOf(ExplainAnalyzeNode.class)
            .matching(explainAnalyzeNode -> !explainAnalyzeNode.getStatsAndCosts().isPresent())
            .matching(ExplainAnalyzeNode::isVerbose);

    @Override
    public Pattern<ExplainAnalyzeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ExplainAnalyzeNode node, Captures captures, Context context)
    {
        ImmutableMap.Builder<PlanNodeId, ExplainPlanNodeStatsAndCost> stats = ImmutableMap.builder();
        exploreNode(context.getLookup().resolve(node.getSource()), stats, context);
        return Result.ofPlanNode(new ExplainAnalyzeNode(node.getId(), node.getSource(), node.getOutputSymbol(), Optional.of(stats.build()), node.isVerbose()));
    }

    private void exploreNode(PlanNode node, ImmutableMap.Builder<PlanNodeId, ExplainPlanNodeStatsAndCost> stats, Context context)
    {
        PlanNodeStatsEstimate statsEstimate = context.getStatsProvider().getStats(node);
        PlanNodeCostEstimate costEstimate = context.getCostProvider().getCumulativeCost(node);
        ExplainPlanNodeStatsAndCost nodeStats = new ExplainPlanNodeStatsAndCost(
                statsEstimate.getOutputRowCount(),
                statsEstimate.getOutputSizeInBytes(node.getOutputSymbols()),
                costEstimate.getCpuCost(),
                costEstimate.getMemoryCost(),
                costEstimate.getNetworkCost());
        stats.put(node.getId(), nodeStats);
        node.getSources().stream()
                .map(child -> context.getLookup().resolve(child))
                .forEach(child -> exploreNode(child, stats, context));
    }
}
