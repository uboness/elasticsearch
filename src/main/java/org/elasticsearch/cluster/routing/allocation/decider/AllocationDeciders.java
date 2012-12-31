/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Set;

/**
 * A composite {@link AllocationDecider} combining the "decision" of multiple
 * {@link AllocationDecider} implementations into a single allocation decision.
 */
public class AllocationDeciders extends AllocationDecider {

    private final static String NAME = "allocation deciders";

    private final AllocationDecider[] deciders;

    /**
     * Create a new {@link AllocationDeciders} instance
     * @param settings  settings to use
     * @param nodeSettingsService per-node settings to use
     */
    public AllocationDeciders(Settings settings, NodeSettingsService nodeSettingsService) {
        this(settings, ImmutableSet.<AllocationDecider>builder()
                .add(new SameShardAllocationDecider(settings))
                .add(new FilterAllocationDecider(settings, nodeSettingsService))
                .add(new ReplicaAfterPrimaryActiveAllocationDecider(settings))
                .add(new ThrottlingAllocationDecider(settings, nodeSettingsService))
                .add(new RebalanceOnlyWhenActiveAllocationDecider(settings))
                .add(new ClusterRebalanceAllocationDecider(settings))
                .add(new ConcurrentRebalanceAllocationDecider(settings, nodeSettingsService))
                .add(new DisableAllocationDecider(settings, nodeSettingsService))
                .add(new AwarenessAllocationDecider(settings, nodeSettingsService))
                .add(new ShardsLimitAllocationDecider(settings))
                .build()
        );
    }

    @Inject
    public AllocationDeciders(Settings settings, Set<AllocationDecider> deciders) {
        super(NAME, settings);
        this.deciders = deciders.toArray(new AllocationDecider[deciders.size()]);
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        Decision.Multi ret = new Decision.Multi(NAME);
        for (AllocationDecider allocationDecider : deciders) {
            Decision decision = allocationDecider.canRebalance(shardRouting, allocation);
            if (decision != Decision.NOT_APPLICABLE) {
                ret.add(decision);
            }

            // we can shortcut here if the decision is no
            if (decision.type() == Decision.Type.NO) {
                allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation("rebalance", Decision.NO));
                return ret;
            }
        }
        allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation("rebalance", ret));
        return ret;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation(node.node(), "allocate", Decision.NO));
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi(NAME);
        for (AllocationDecider allocationDecider : deciders) {
            Decision decision = allocationDecider.canAllocate(shardRouting, node, allocation);
            // the assumption is that a decider that returns the static instance Decision#NOT_APPLICABLE
            // does not really implements canAllocate
            if (decision != Decision.NOT_APPLICABLE) {
                ret.add(decision);
            }

            // we can shortcut here if the decision is no
            if (decision.type() == Decision.Type.NO) {
                allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation(node.node(), "allocate", ret));
                return ret;
            }
        }
        allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation(node.node(), "allocate", ret));
        return ret;
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (allocation.shouldIgnoreShardForNode(shardRouting.shardId(), node.nodeId())) {
            allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation(node.node(), "remain", Decision.NO));
            return Decision.NO;
        }
        Decision.Multi ret = new Decision.Multi(NAME);
        for (AllocationDecider allocationDecider : deciders) {
            Decision decision = allocationDecider.canRemain(shardRouting, node, allocation);
            if (decision != Decision.NOT_APPLICABLE) {
                ret.add(decision);
            }
            // we can shortcut here if the decision is no
            if (decision.type() == Decision.Type.NO) {
                allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation(node.node(), "remain", ret));
                return ret;
            }
        }
        allocation.explanation().add(shardRouting.shardId(), new AllocationExplanation.Explanation(node.node(), "remain", ret));
        return ret;
    }
}
