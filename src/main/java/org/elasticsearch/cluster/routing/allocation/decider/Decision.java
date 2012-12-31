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

import com.google.common.collect.Lists;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * This abstract class defining basic {@link Decision} used during shard
 * allocation process.
 * 
 * @see AllocationDecider
 */
public abstract class Decision implements ToXContent {

    private static final ESLogger logger = ESLoggerFactory.getLogger(Decision.class.getName());

    public static final Decision NOT_APPLICABLE = new Single(null, Type.YES, "");
    public static final Decision NO = new Single(null, Type.NO, "");


    public static Decision single(Type type, String title, String explanation, Object... explanationParams) {
        return new Single(title, type, explanation, explanationParams);
    }

    public static Decision yes(String title, String explanation, Object... explanationParams) {
        return new Single(title, Type.YES, explanation, explanationParams);
    }

    public static Decision no(String title, String explanation, Object... explanationParams) {
        return new Single(title, Type.NO, explanation, explanationParams);
    }

    public static Decision throttle(String title, String explanation, Object... explanationParams) {
        return new Single(title, Type.THROTTLE, explanation, explanationParams);
    }

    public static Decision readDecision(StreamInput in) throws IOException {
        boolean isMulti = in.readBoolean();
        Type type = Type.valueOf(in.readString());
        String title = in.readString();
        if (isMulti) {
            Multi multi = new Multi(title);
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                multi.add(readDecision(in));
            }
            return multi;
        }
        return new Single(title, type, in.readString());
    }

    public static void writeDecision(Decision decision, StreamOutput out) throws IOException {
        boolean isMulti = decision instanceof Multi;
        out.writeBoolean(isMulti);
        out.writeString(decision.type().name());
        out.writeString(decision.title());
        if (isMulti) {
            Multi multi = (Multi) decision;
            out.writeInt(multi.decisions.size());
            for (Decision sub : multi.decisions) {
                writeDecision(sub, out);
            }
        } else {
            out.writeString(decision.toString());
        }
    }

    /**
     * This enumeration defines the 
     * possible types of decisions 
     */
    public static enum Type {
        YES,
        NO,
        THROTTLE
    }

    protected final String title;

    Decision(String title) {
        this.title = title;
    }

    public String title() {
        return title;
    }

    /**
     * @return {@link Type} of this decision
     */
    public abstract Type type();

    /**
     * @return The explanation for this decision
     */
    public abstract String explanation();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (title != null) {
            sb.append('[').append(title).append(']').append(": ");
        }
        sb.append(type());
        if (logger.isTraceEnabled()) {
            String explanation = explanation();
            if (Strings.hasText(explanation)) {
                sb.append(" (").append(explanation).append(')');
            }
        }
        return sb.toString();
    }

    /**
     * Simple class representing a single decision
     */
    public static class Single extends Decision {

        private final Type type;
        private final String explanation;
        private final Object[] explanationParams;

        /**
         * Creates a new {@link Single} decision of a given type 
         * @param type {@link Type} of the decision
         */
        public Single(String title, Type type) {
            this(title, type, null, (Object[]) null);
        }

        /**
         * Creates a new {@link Single} decision of a given type
         *  
         * @param type {@link Type} of the decision
         * @param explanation An explanation of this {@link Decision}
         * @param explanationParams A set of additional parameters
         */
        public Single(String title, Type type, String explanation, Object... explanationParams) {
            super(title);
            this.type = type;
            this.explanation = explanation;
            this.explanationParams = explanationParams;
        }

        @Override
        public Type type() {
            return this.type;
        }

        @Override
        public String explanation() {
            return String.format(explanation, explanationParams);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("decider", title);
            builder.field("result", type.name());
            builder.field("reason", explanation());
            return builder.endObject();
        }
    }

    /**
     * Simple class representing a list of decisions
     */
    public static class Multi extends Decision {

        private final static String DEFAULT_TITLE = "allocation deciders";

        private final List<Decision> decisions = Lists.newArrayList();

        public Multi() {
            super(DEFAULT_TITLE);
        }

        public Multi(String title) {
            super(title);
        }

        /**
         * Add a decission to this {@link Multi}decision instance
         * @param decision {@link Decision} to add
         * @return {@link Multi}decision instance with the given decision added
         */
        public Multi add(Decision decision) {
            decisions.add(decision);
            return this;
        }

        public List<Decision> decisions() {
            return decisions;
        }

        @Override
        public Type type() {
            Type ret = Type.YES;
            for (int i = 0; i < decisions.size(); i++) {
                Type type = decisions.get(i).type();
                if (type == Type.NO) {
                    return type;
                } else if (type == Type.THROTTLE) {
                    ret = type;
                }
            }
            return ret;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("decider", title);
            builder.field("result", type().name());
            builder.startArray("reason");
            for (Decision sub : decisions) {
                sub.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public String explanation() {
            return explanation(0);
        }

        private String explanation(int indent) {
            StringBuilder sb = new StringBuilder();
            for (Decision decision : decisions) {
                if (decision.type() != Type.YES) {
                    sb.append('\n');
                    Strings.appendSpaceTabs(indent+1, sb);
                    if (decision instanceof Multi) {
                        sb.append(((Multi) decision).toString(indent + 1));
                    } else {
                        sb.append(decision);
                    }
                }
            }
            return sb.toString();
        }


        @Override
        public String toString() {
            return toString(0);
        }

        private String toString(int indent) {
            StringBuilder sb = new StringBuilder();
            if (title != null) {
                sb.append('[').append(title).append(']').append(": ");
            }
            sb.append(type());
            if (logger.isTraceEnabled()) {
                String explanation = explanation(indent);
                if (Strings.hasText(explanation)) {
                    sb.append(" (").append(explanation).append(')');
                }
            }
            return sb.toString();
        }

    }
}
