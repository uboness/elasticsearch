/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.percentile;

import com.carrotsearch.hppc.DoubleArrayList;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.numeric.NumericValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class PercentileParser implements Aggregator.Parser {

    private final static double[] DEFAULT_PERCENTS = new double[] { 1, 5, 25, 50, 75, 95, 99 };

    @Override
    public String type() {
        return InternalPercentiles.TYPE.name();
    }

    /**
     * We must override the parse method because we need to allow custom parameters
     * (execution_hint, etc) which is not possible otherwise
     */
    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceConfig<NumericValuesSource> config = new ValuesSourceConfig<NumericValuesSource>(NumericValuesSource.class);

        String field = null;
        String script = null;
        String scriptLang = null;
        InternalExecutionHint executionHint = InternalExecutionHint.TDIGEST;
        double[] percents = DEFAULT_PERCENTS;
        Map<String, Object> scriptParams = null;
        boolean assumeSorted = false;
        boolean keyed = true;
        Map<String, Object> settings = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("field".equals(currentFieldName)) {
                    field = parser.text();
                } else if ("script".equals(currentFieldName)) {
                    script = parser.text();
                } else if ("lang".equals(currentFieldName)) {
                    scriptLang = parser.text();
                } else if ("execution_hint".equals(currentFieldName)) {
                    executionHint = InternalExecutionHint.resolve(parser.text());
                    if (executionHint == null) {
                        throw new SearchParseException(context, "Unknown percentile execution_hint [" + parser.text() + "]");
                    }
                } else {
                    if (settings == null) {
                        settings = new HashMap<String, Object>();
                    }
                    settings.put(currentFieldName, parser.text());
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("percents".equals(currentFieldName)) {
                    DoubleArrayList values = new DoubleArrayList(10);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double percent = parser.doubleValue();
                        if (percent < 0 || percent > 100) {
                            throw new SearchParseException(context, "the percents in the percentiles aggregation [" +
                                    aggregationName + "] must be in the [0, 100] range");
                        }
                        values.add(percent);
                    }
                    percents = values.toArray();
                    // Some impls rely on the fact that percents are sorted
                    Arrays.sort(percents);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentFieldName)) {
                    scriptParams = parser.map();
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if ("script_values_sorted".equals(currentFieldName)) {
                    assumeSorted = parser.booleanValue();
                } if ("keyed".equals(currentFieldName)) {
                    keyed = parser.booleanValue();
                } else {
                    if (settings == null) {
                        settings = new HashMap<String, Object>();
                    }
                    settings.put(currentFieldName, parser.booleanValue());
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (settings == null) {
                    settings = new HashMap<String, Object>();
                }
                settings.put(currentFieldName, parser.numberValue());
            }
        }

        PercentilesEstimator.Factory estimatorFactory = executionHint.estimatorFactory(settings);

        if (script != null) {
            config.script(context.scriptService().search(context.lookup(), scriptLang, script, scriptParams));
        }

        if (!assumeSorted) {
            config.ensureSorted(true);
        }

        if (field == null) {
            return new PercentileAggregator.Factory(aggregationName, config, percents, estimatorFactory, keyed);
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return new PercentileAggregator.Factory(aggregationName, config, percents, estimatorFactory, keyed);
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);
        config.fieldContext(new FieldContext(field, indexFieldData));
        return new PercentileAggregator.Factory(aggregationName, config, percents, estimatorFactory, keyed);
    }

}
