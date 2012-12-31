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

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class DiscoveryNodeFilters {

    public static enum OpType {
        AND,
        OR
    }

    public static class Match {

        private final boolean matches;
        private final String explanation;
        private final Object[] explanationParams;

        public final static Match YES = yes("");
        public final static Match NO = no("");

        private Match(boolean matches, String explanation, Object... explanationParams) {
            this.matches = matches;
            this.explanation = explanation;
            this.explanationParams = explanationParams;
        }

        public static Match yes(String explanation, Object... explanationParams) {
            return new Match(true, explanation, explanationParams);
        }

        public static Match no(String explanation, Object... explanationParams) {
            return new Match(false, explanation, explanationParams);
        }

        public boolean matches() {
            return matches;
        }

        public String explanation() {
            return explanation;
        }

        public Object[] explanationParams() {
            return explanationParams;
        }

        public String toString() {
            return String.format(explanation, explanationParams);
        }
    }

    public static DiscoveryNodeFilters buildFromSettings(OpType opType, String prefix, Settings settings) {
        return buildFromKeyValue(opType, settings.getByPrefix(prefix).getAsMap());
    }

    public static DiscoveryNodeFilters buildFromKeyValue(OpType opType, Map<String, String> filters) {
        Map<String, String[]> bFilters = new HashMap<String, String[]>();
        for (Map.Entry<String, String> entry : filters.entrySet()) {
            String[] values = Strings.splitStringByCommaToArray(entry.getValue());
            if (values.length > 0) {
                bFilters.put(entry.getKey(), values);
            }
        }
        if (bFilters.isEmpty()) {
            return null;
        }
        return new DiscoveryNodeFilters(opType, bFilters);
    }

    private final Map<String, String[]> filters;

    private final OpType opType;

    DiscoveryNodeFilters(OpType opType, Map<String, String[]> filters) {
        this.opType = opType;
        this.filters = filters;
    }

    public Match match(DiscoveryNode node) {
        return match(node, false);
    }

    public Match match(DiscoveryNode node, boolean detailed) {
        for (Map.Entry<String, String[]> entry : filters.entrySet()) {
            String attr = entry.getKey();
            String[] values = entry.getValue();
            if ("_ip".equals(attr)) {
                if (!(node.address() instanceof InetSocketTransportAddress)) {
                    if (opType == OpType.AND) {
                        if (detailed) {
                            return Match.no("node [%s[%s]] doesn't confirm to required _ip filters [%s] as it is not socket bound", node.id(), node.address(), Strings.arrayToCommaDelimitedString(values));
                        }
                        return Match.NO;
                    } else {
                        continue;
                    }
                }
                InetSocketTransportAddress inetAddress = (InetSocketTransportAddress) node.address();
                for (String value : values) {
                    if (Regex.simpleMatch(value, inetAddress.address().getAddress().getHostAddress())) {
                        if (opType == OpType.OR) {
                            if (detailed) {
                                return Match.yes("node [%s[%s]] confirms to one of the _ip filters [%s]", node.id(), inetAddress.address().getAddress().getHostAddress(), value);
                            }
                            return Match.YES;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            if (detailed) {
                                return Match.no("node [%s[%s]] doesn't confirm to the required _ip filter [%s]", node.id(), inetAddress.address().getAddress().getHostAddress(), value);
                            }
                            return Match.NO;
                        }
                    }
                }
            } else if ("_host".equals(attr)) {
                if (!(node.address() instanceof InetSocketTransportAddress)) {
                    if (opType == OpType.AND) {
                        if (detailed) {
                            return Match.no("node [%s[%s]] doesn't confirm to required _host filters [%s] as it is not socket bound", node.id(), node.address(), Strings.arrayToCommaDelimitedString(values));
                        }
                        return Match.NO;
                    } else {
                        continue;
                    }
                }
                InetSocketTransportAddress inetAddress = (InetSocketTransportAddress) node.address();
                for (String value : values) {
                    if (Regex.simpleMatch(value, inetAddress.address().getHostName())) {
                        if (opType == OpType.OR) {
                            if (detailed) {
                                return Match.yes("node [%s[%s]] confirms to one of the _host filters [%s]", node.id(), inetAddress.address().getHostName(), value);
                            }
                            return Match.YES;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            if (detailed) {
                                return Match.no("node [%s[%s]] doesn't confirm to the required _host filter [%s]", node.id(), inetAddress.address().getHostName(), value);
                            }
                            return Match.NO;
                        }
                    }
                    if (Regex.simpleMatch(value, inetAddress.address().getAddress().getHostAddress())) {
                        if (opType == OpType.OR) {
                            if (detailed) {
                                return Match.yes("node [%s[%s]] confirms to one of the _host filters [%s]", node.id(), inetAddress.address().getAddress().getHostAddress(), value);
                            }
                            return Match.YES;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            if (detailed) {
                                return Match.no("node [%s[%s]] doesn't confirm to the required _host filter [%s]", node.id(), inetAddress.address().getAddress().getHostAddress(), value);
                            }
                            return Match.NO;
                        }
                    }
                }
            } else if ("_id".equals(attr)) {
                for (String value : values) {
                    if (node.id().equals(value)) {
                        if (opType == OpType.OR) {
                            if (detailed) {
                                return Match.yes("node [%s] matches one of the _id filters [%s]", node.id(), value);
                            }
                            return Match.YES;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            if (detailed) {
                                return Match.no("node [%s] doesn't match a required _id filter [%s]", node.id(), value);
                            }
                            return Match.NO;
                        }
                    }
                }
            } else if ("_name".equals(attr) || "name".equals(attr)) {
                for (String value : values) {
                    if (Regex.simpleMatch(value, node.name())) {
                        if (opType == OpType.OR) {
                            if (detailed) {
                                return Match.yes("node [%s[%s]] matches one of the _name filters [%s]", node.id(), node.name(), value);
                            }
                            return Match.YES;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            if (detailed) {
                                return Match.yes("node [%s[%s]] doesn't match a required _name filters [%s]", node.id(), node.name(), value);
                            }
                            return Match.YES;
                        }
                    }
                }
            } else {
                String nodeAttributeValue = node.attributes().get(attr);
                if (nodeAttributeValue == null) {
                    if (opType == OpType.AND) {
                        if (detailed) {
                            return Match.no("node [%s%s] doesn't have a required metadata attribute [%s]", node.id(), node.attributes(), attr);
                        }
                        return Match.NO;
                    } else {
                        continue;
                    }
                }
                for (String value : values) {
                    if (Regex.simpleMatch(value, nodeAttributeValue)) {
                        if (opType == OpType.OR) {
                            if (detailed) {
                                return Match.yes("node [%s%s] matches one of the metadata attributes filters - [\"%s:%s\"]", node.id(), node.attributes(), attr, value);
                            }
                            return Match.YES;
                        }
                    } else {
                        if (opType == OpType.AND) {
                            if (detailed) {
                                return Match.no("node [%s%s] doesn't match a required metadata attribute filter [\"%s:%s\"]", node.id(), node.attributes(), attr, value);
                            }
                            return Match.NO;
                        }
                    }
                }
            }
        }
        if (opType == OpType.OR) {
            if (detailed) {
                return Match.no("node [%s%s] doesn't match any of the filters [%s]", node.id(), node.attributes(), Strings.mapToCommaDelimitedString(filters, "\"", "\"", "", "", ":", ","));
            }
            return Match.NO;
        }

        if (detailed) {
            return Match.yes("node [%s%s] matched all filters [%s]", node.id(), node.attributes(), Strings.mapToCommaDelimitedString(filters, "\"", "\"", "", "", ":", ","));
        }

        return Match.YES;
    }
}