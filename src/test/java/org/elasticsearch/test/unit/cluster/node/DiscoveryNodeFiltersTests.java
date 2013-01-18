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

package org.elasticsearch.test.unit.cluster.node;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeFilters;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.*;

/**
 */
@Test
public class DiscoveryNodeFiltersTests {

    @Test
    public void nameMatch() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("xxx.name", "name1")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        DiscoveryNodeFilters.Match match = filters.match(node, true);
        assertThat(match.matches(), equalTo(true));
        System.out.println(match);

        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        match = filters.match(node, true);
        assertThat(match.matches(), equalTo(false));
        System.out.println(match);
    }

    @Test
    public void idMatch() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("xxx._id", "id1")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        assertThat(filters.match(node).matches(), equalTo(true));

        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        DiscoveryNodeFilters.Match match = filters.match(node, true);
        assertThat(match.matches(), equalTo(false));
        System.out.println(match);
    }

    @Test
    public void idOrNameMatch() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("xxx._id", "id1,blah")
                .put("xxx.name", "blah,name2")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        DiscoveryNodeFilters.Match match = filters.match(node, true);
        assertThat(match.matches(), equalTo(true));
        System.out.println(match);

        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        match = filters.match(node, true);
        assertThat(match.matches(), equalTo(true));
        System.out.println(match);

        node = new DiscoveryNode("name3", "id3", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        match = filters.match(node, true);
        assertThat(match.matches(), equalTo(false));
        System.out.println(match);
    }

    @Test
    public void tagAndGroupMatch() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("xxx.tag", "A")
                .put("xxx.group", "B")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE,
                ImmutableMap.<String, String>of("tag", "A", "group", "B"));
        DiscoveryNodeFilters.Match match = filters.match(node, true);
        assertThat(match.matches(), equalTo(true));
        System.out.println(match);

        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE,
                ImmutableMap.<String, String>of("tag", "A", "group", "B", "name", "X"));
        match = filters.match(node, true);
        assertThat(match.matches(), equalTo(true));
        System.out.println(match);

        node = new DiscoveryNode("name3", "id3", DummyTransportAddress.INSTANCE,
                ImmutableMap.<String, String>of("tag", "A", "group", "F", "name", "X"));
        match = filters.match(node, true);
        assertThat(match.matches(), equalTo(false));
        System.out.println(match);

        node = new DiscoveryNode("name4", "id4", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        match = filters.match(node, true);
        assertThat(match.matches(), equalTo(false));
        System.out.println(match);
    }

    @Test
    public void starMatch() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("xxx.name", "*")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, ImmutableMap.<String, String>of());
        DiscoveryNodeFilters.Match match = filters.match(node, true);
        assertThat(match.matches(), equalTo(true));
        System.out.println(match);
    }
}
