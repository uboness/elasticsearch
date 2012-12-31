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

import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * This {@link AllocationDecider} controls re-balancing operations based on the
 * cluster wide active shard state. This decider can not be configured in
 * real-time and should be pre-cluster start via
 * <tt>cluster.routing.allocation.allow_rebalance</tt>. This setting respects the following
 * values:
 * <ul>
 * <li><tt>indices_primaries_active</tt> - Re-balancing is allowed only once all
 * primary shards on all indices are active.</li>
 * 
 * <li><tt>indices_all_active</tt> - Re-balancing is allowed only once all
 * shards on all indices are active.</li>
 * 
 * <li><tt>always</tt> - Re-balancing is allowed once a shard replication group
 * is active (that is, at least on shard of one index is active)</li>
 * </ul>
 */
public class ClusterRebalanceAllocationDecider extends AllocationDecider {

    private final static String NAME = "allow rebalance";

    /**
     * An enum representation for the configured re-balance type. 
     */
    public static enum ClusterRebalanceType {
        /**
         * Re-balancing is allowed once a shard replication group is active
         */
        ALWAYS,
        /**
         * Re-balancing is allowed only once all primary shards on all indices are active.
         */
        INDICES_PRIMARIES_ACTIVE,
        /**
         * Re-balancing is allowed only once all shards on all indices are active. 
         */
        INDICES_ALL_ACTIVE
    }


    private final static Decision NO = Decision.no(NAME, "");
    private final static Decision ALWAYS_YES_DECISION = Decision.yes(NAME, "[cluster.routing.allocation.allow_rebalance] is set to [always]");
    private final static Decision INDICES_PRIMARIES_ACTIVE_YES_DECISION = Decision.yes(NAME, "[cluster.routing.allocation.allow_rebalance] is set to [indices_all_active] and all primary shards are active");
    private final static Decision INDICES_ALL_ACTIVE_YES_DECISION = Decision.yes(NAME, "[cluster.routing.allocation.allow_rebalance] is set to [indices_all_active] and all shards are active");


    private final ClusterRebalanceType type;

    @Inject
    public ClusterRebalanceAllocationDecider(Settings settings) {
        super(NAME, settings);
        String allowRebalance = settings.get("cluster.routing.allocation.allow_rebalance", "indices_all_active");
        if ("always".equalsIgnoreCase(allowRebalance)) {
            type = ClusterRebalanceType.ALWAYS;
        } else if ("indices_primaries_active".equalsIgnoreCase(allowRebalance) || "indicesPrimariesActive".equalsIgnoreCase(allowRebalance)) {
            type = ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE;
        } else if ("indices_all_active".equalsIgnoreCase(allowRebalance) || "indicesAllActive".equalsIgnoreCase(allowRebalance)) {
            type = ClusterRebalanceType.INDICES_ALL_ACTIVE;
        } else {
            logger.warn("[cluster.routing.allocation.allow_rebalance] has a wrong value {}, defaulting to 'indices_all_active'", allowRebalance);
            type = ClusterRebalanceType.INDICES_ALL_ACTIVE;
        }
        logger.debug("using [cluster.routing.allocation.allow_rebalance] with [{}]", type.toString().toLowerCase());
    }

    @Override
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        if (type == ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE) {
            for (MutableShardRouting shard : allocation.routingNodes().unassigned()) {
                if (shard.primary()) {
                    return allocation.decisionDebug(NO, "[cluster.routing.allocation.allow_rebalance] is set to [indices_all_active] and primary shard %s is unassigned", shard.shardId());
                }
            }
            for (RoutingNode node : allocation.routingNodes()) {
                List<MutableShardRouting> shards = node.shards();
                for (int i = 0; i < shards.size(); i++) {
                    MutableShardRouting shard = shards.get(i);
                    if (shard.primary() && !shard.active() && shard.relocatingNodeId() == null) {
                        return allocation.decisionDebug(NO, "[cluster.routing.allocation.allow_rebalance] is set to [indices_all_active] and primary shard %s is not active", shard.shardId());
                    }
                }
            }
            return INDICES_PRIMARIES_ACTIVE_YES_DECISION;
        }

        if (type == ClusterRebalanceType.INDICES_ALL_ACTIVE) {
            if (!allocation.routingNodes().unassigned().isEmpty()) {
                return allocation.decisionDebug(NO, "[cluster.routing.allocation.allow_rebalance] is set to [indices_all_active] and not all shards are assigned");
            }
            for (RoutingNode node : allocation.routingNodes()) {
                List<MutableShardRouting> shards = node.shards();
                for (int i = 0; i < shards.size(); i++) {
                    MutableShardRouting shard = shards.get(i);
                    if (!shard.active() && shard.relocatingNodeId() == null) {
                        return allocation.decisionDebug(NO, "[cluster.routing.allocation.allow_rebalance] is set to [indices_all_active] and shard %s is not active", shard.shardId());
                    }
                }
            }
            return INDICES_ALL_ACTIVE_YES_DECISION;
        }

        // type == Type.ALWAYS
        return ALWAYS_YES_DECISION;
    }
}
