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

import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

/**
 * {@link AllocationDecider} is an abstract base class that allows to make
 * dynamic cluster- or index-wide shard allocation decisions on a per-node
 * basis.
 */
public abstract class AllocationDecider extends AbstractComponent {

    private final String name;

    /**
     * Initializes a new {@link AllocationDecider}
     * @param settings {@link Settings} used by this {@link AllocationDecider}
     */
    protected AllocationDecider(String name, Settings settings) {
        super(settings);
        this.name = name;
    }

    /**
     * @return The name of this allocation decider
     */
    public String name() {
        return name;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * re-balanced within the given allocation. The default is
     * {@link Decision#NOT_APPLICABLE}.
     */
    public Decision canRebalance(ShardRouting shardRouting, RoutingAllocation allocation) {
        return Decision.NOT_APPLICABLE;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be
     * allocated on the given node. The default is {@link Decision#NOT_APPLICABLE}.
     */
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.NOT_APPLICABLE;
    }

    /**
     * Returns a {@link Decision} whether the given shard routing can be remain
     * on the given node. The default is {@link Decision#NOT_APPLICABLE}.
     */
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return Decision.NOT_APPLICABLE;
    }
}
