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

package org.elasticsearch.cluster.routing.allocation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Instances of this class keeps explanations of decisions that have been made by allocation.
 * An {@link AllocationExplanation} consists of a set of per node explanations.
 * Since {@link org.elasticsearch.cluster.routing.allocation.AllocationExplanation.Explanation}s are related to shards an {@link AllocationExplanation} maps
 * a shards id to a set of {@link org.elasticsearch.cluster.routing.allocation.AllocationExplanation.Explanation}s.
 */
public class AllocationExplanation implements Streamable, ToXContent {

    public static final AllocationExplanation EMPTY = new AllocationExplanation();

    public enum Level {

        INFO, DEBUG, TRACE;

        public boolean debugEnabled() {
            return this != INFO;
        }

        public boolean traceEnabled() {
            return this == TRACE;
        }

    }

    /**
     * Instances of this class keep messages and informations about nodes of an allocation
     */
    public static class Explanation {

        private final DiscoveryNode node;
        private final String action;
        private final Decision decision;

        public Explanation(String action, Decision decision) {
            this(null, action, decision);
        }

        /**
         * Creates a new {@link org.elasticsearch.cluster.routing.allocation.AllocationExplanation.Explanation}
         *  
         * @param node node referenced by {@link this} {@link org.elasticsearch.cluster.routing.allocation.AllocationExplanation.Explanation}
         * @param action The allocation action
         * @param decision A decision indicating whether the shard could be allocated on the given node
         */
        public Explanation(DiscoveryNode node, String action, Decision decision) {
            this.node = node;
            this.action = action;
            this.decision = decision;
        }

        /**
         * The node referenced by the explanation
         * @return referenced node
         */
        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        /**
         * @return allocation decision for the node
         */
        public Decision decision() {
            return decision;
        }

        @Override
        public String toString() {
            if (node == null) {
                return String.format("action [%s], decision: [%s]", action, decision);
            }
            return String.format("node [%s], action [%s], decision: [%s]", node.id(), action, decision);
        }
    }

    private final Map<ShardId, List<Explanation>> explanations = Maps.newHashMap();

    private Level level;

    public AllocationExplanation() {
        this(Level.INFO);
    }

    public AllocationExplanation(Level level) {
        this.level = level;
    }

    /**
     * Create and add a node explanation to this explanation referencing a shard  
     * @param shardId id the of the referenced shard
     * @param explanation Explanation itself
     * @return AllocationExplanation involving the explanation 
     */
    public AllocationExplanation add(ShardId shardId, Explanation explanation) {
        List<Explanation> list = explanations.get(shardId);
        if (list == null) {
            list = Lists.newArrayList();
            explanations.put(shardId, list);
        }
        list.add(explanation);
        return this;
    }

    public Level level() {
        return level;
    }

    /**
     * List of explanations involved by this AllocationExplanation
     * @return Map of shard ids and corresponding explanations  
     */
    public Map<ShardId, List<Explanation>> explanations() {
        return this.explanations;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("allocation explanation:");
        for (Map.Entry<ShardId, List<Explanation>> entry : explanations.entrySet()) {
            for (Explanation explanation : entry.getValue()) {
                sb.append('\n').append("shard ").append(entry.getKey()).append(", ").append(explanation);
            }
        }
        return sb.toString();
    }

    /**
     * Read an {@link AllocationExplanation} from an {@link StreamInput}
     * @param in {@link StreamInput} to read from
     * @return a new {@link AllocationExplanation} read from the stream 
     * @throws IOException if something bad happened while reading
     */
    public static AllocationExplanation readAllocationExplanation(StreamInput in) throws IOException {
        AllocationExplanation e = new AllocationExplanation();
        e.readFrom(in);
        return e;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        level = Level.valueOf(in.readString());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            ShardId shardId = ShardId.readShardId(in);
            int size2 = in.readVInt();
            List<Explanation> ne = Lists.newArrayListWithCapacity(size2);
            for (int j = 0; j < size2; j++) {
                DiscoveryNode node = null;
                if (in.readBoolean()) {
                    node = DiscoveryNode.readNode(in);
                }
                ne.add(new Explanation(node, in.readString(), Decision.readDecision(in)));
            }
            explanations.put(shardId, ne);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(level.name());
        out.writeVInt(explanations.size());
        for (Map.Entry<ShardId, List<Explanation>> entry : explanations.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeVInt(entry.getValue().size());
            for (Explanation explanation : entry.getValue()) {
                if (explanation.node() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    explanation.node().writeTo(out);
                }
                out.writeString(explanation.action);
                Decision.writeDecision(explanation.decision, out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (Map.Entry<ShardId, List<Explanation>> entry : explanations.entrySet()) {
            for (Explanation explanation : entry.getValue()) {
                builder.startObject();
                builder.field("action", explanation.action());
                builder.field("index", entry.getKey().getIndex());
                builder.field("shard", entry.getKey().getId());
                if (explanation.node != null) {
                    builder.field("node_id", explanation.node.id());
                    builder.field("node_name", explanation.node.name());
                }
                builder.field("decision");
                if ("basic".equalsIgnoreCase(params.param("explain", "basic"))) {
                    builder.value(explanation.decision.type().name());
                } else { // detailed
                    explanation.decision.toXContent(builder, params);
                }

                builder.endObject();
            }
        }
        builder.endArray();
        return builder;
    }
}
