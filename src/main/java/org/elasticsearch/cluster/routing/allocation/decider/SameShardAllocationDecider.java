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
 * An allocation decider that prevents multiple instances
 * of the same shard to be allocated on a single <tt>host</tt>. The cluster setting can
 * be modified in real-time by updating the {@value #SAME_HOST_SETTING} value of
 * cluster setting API. The default is <code>false</code>.
 * <p>
 * Note: this setting only applies if multiple nodes are started on the same
 * <tt>host</tt>. Multiple allocations of the same shard on the same <tt>node</tt> are
 * not allowed independent of this setting.
 * </p>
 */
public class SameShardAllocationDecider extends AllocationDecider {

    private final static String NAME = "same shard";

    public static final String SAME_HOST_SETTING = "cluster.routing.allocation.same_shard.host";

    private static final Decision NO = Decision.no(NAME, "");
    private static final Decision YES = Decision.yes(NAME, "");
    private static final Decision SAME_NODE_DECISION = Decision.no(NAME, "two shards of the same shard id cannot be allocated on the same node");

    private final boolean sameHost;

    @Inject
    public SameShardAllocationDecider(Settings settings) {
        super(NAME, settings);

        this.sameHost = settings.getAsBoolean(SAME_HOST_SETTING, false);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        List<MutableShardRouting> shards = node.shards();
        for (int i = 0; i < shards.size(); i++) {
            // we do not allow for two shards of the same shard id to exists on the same node
            if (shards.get(i).shardId().equals(shardRouting.shardId())) {
                return SAME_NODE_DECISION;
            }
        }

        if (!sameHost) {
            return allocation.decisionTrace(YES, "no other shard with shard id [%s] is currently allocated on node [%s]", shardRouting.id(), node.nodeId());
        }

        if (node.node() != null) {
            for (RoutingNode checkNode : allocation.routingNodes()) {
                if (checkNode.node() == null) {
                    continue;
                }
                // check if its on the same host as the one we want to allocate to
                if (!checkNode.node().address().sameHost(node.node().address())) {
                    continue;
                }
                shards = checkNode.shards();
                for (int i = 0; i < shards.size(); i++) {
                    ShardRouting shard = shards.get(i);
                    if (shard.shardId().equals(shardRouting.shardId())) {
                        return allocation.decisionDebug(NO, "there's already a shard with shard id [%s] assigned to the node [%s] host [%s] ([cluster.routing.allocation.same_shard.host] is set to [true])", shard.id(), node.nodeId(), node.node().address());
                    }
                }
            }

            return allocation.decisionTrace(YES, "no other shard with shard id [%s] is allocated on the same node [%s] or the same host [%s]", shardRouting.id(), node.nodeId(), node.node().address());
        }

        return allocation.decisionTrace(YES, "no other shard with shard id [%s] is allocated on the same node [%s]", shardRouting.id(), node.nodeId());
    }
}
