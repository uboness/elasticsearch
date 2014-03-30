/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
public class DefaultValuesTests extends ElasticsearchIntegrationTest {

    int missingCount;
    int existCount;
    int docCount;

    @Override
    protected int numberOfShards() {
        return 3;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Before
    public void init() throws Exception {

        existCount = randomIntBetween(1, 10);
        missingCount = randomIntBetween(1, 10);
        docCount = missingCount + existCount;

        createIndex("idx");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[docCount];
        for (int i = 0; i < existCount; i++) {
            builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("foo", "bar")
                    .field("svalue", "val" + i)
                    .field("lvalue", i)
                    .field("dvalue", (double) i)
                    .endObject());
        }
        for (int i = 0; i < missingCount; i++) {
            builders[existCount + i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("foo", "bar")
                    .endObject());
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");
        ensureSearchable();
    }

    @Test
    public void stringTerms_WithNoValues() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms").field("svalue").defaultValues())
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(existCount));
    }

    @Test
    public void stringTerms_WithSingleDefaultValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                // we need to set the return size to 11. If there are 10 existing values, then the extra bucket
                // for the missing will mean that we have 11 buckets in total. If we keep it 10 (the default),
                // we may miss a bucket
                .addAggregation(terms("terms").field("svalue").defaultValues("_missing").size(11))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(existCount + 1));

        Terms.Bucket missing = terms.getBucketByKey("_missing");
        assertThat(missing, Matchers.notNullValue());
        assertThat(missing.getKey(), equalTo("_missing"));
        assertThat(missing.getDocCount(), equalTo((long) missingCount));
    }

    @Test
    public void stringTerms_WithMultipleDefaultValues() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                // we need to set the return size to 12. If there are 10 existing values, then the extra 2 bucket
                // for the missing values will mean that we have 12 buckets in total. If we keep it 10 (the default),
                // we may miss 2 buckets
                .addAggregation(terms("terms").field("svalue").defaultValues("_missing1", "_missing2").size(12))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(existCount + 2));

        Terms.Bucket missing1 = terms.getBucketByKey("_missing1");
        assertThat(missing1, Matchers.notNullValue());
        assertThat(missing1.getKey(), equalTo("_missing1"));
        assertThat(missing1.getDocCount(), equalTo((long) missingCount));

        Terms.Bucket missing2 = terms.getBucketByKey("_missing2");
        assertThat(missing2, Matchers.notNullValue());
        assertThat(missing2.getKey(), equalTo("_missing2"));
        assertThat(missing2.getDocCount(), equalTo((long) missingCount));
    }
}
