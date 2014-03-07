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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentPushParser;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ValueSourceParser<VS extends ValuesSource> extends XContentPushParser.AbstractCallback<ValuesSourceConfig<VS>> {

    private static final ParseField ASSUME_SORTED = new ParseField("script_values_sorted");
    private static final ParseField ASSUME_UNIQUE = new ParseField("script_values_unique");

    private final SearchContext context;
    private final ValuesSourceConfig<VS> config;
    private final boolean requiresSortedValues;

    String field = null;
    String script = null;
    String scriptLang = null;
    Map<String, Object> scriptParams = null;
    boolean assumeSorted = false;
    boolean assumeUnique = false;

    boolean inParams;

    public ValueSourceParser(Class<VS> valueSourceType, SearchContext context) {
        this(valueSourceType, context, false);
    }

    public ValueSourceParser(Class<VS> valueSourceType, SearchContext context, boolean requiresSortedValues) {
        this.config = new ValuesSourceConfig<VS>(valueSourceType);
        this.context = context;
        this.requiresSortedValues = requiresSortedValues;
    }

    @Override
    protected boolean onValue(String name, XContentParser.Token token, XContentParser parser) throws IOException {
        if (inParams) {
            scriptParams.put(name, value(parser));
            return true;
        }
        if (!currentRoot()) {
            return false;
        }
        if ("field".equals(name)) {
            field = parser.text();
            return true;
        }
        if ("script".equals(name)) {
            script = parser.text();
            return true;
        }
        if ("lang".equals(name)) {
            scriptLang = parser.text();
            return true;
        }
        if (ASSUME_SORTED.match(name)) {
            assumeSorted = parser.booleanValue();
            return true;
        }
        if (ASSUME_UNIQUE.match(name)) {
            assumeUnique = parser.booleanValue();
            return true;
        }
        return false;
    }

    @Override
    protected boolean onObjectStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {
        if ("params".equals(name) && currentRoot()) {
            inParams = true;
            scriptParams = new HashMap<String, Object>();
            return true;
        }
        return false;
    }

    @Override
    protected boolean onObjectEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {
        if ("params".equals(name) && currentRoot()) {
            inParams = false;
            return true;
        }
        return false;
    }

    @Override
    public ValuesSourceConfig<VS> process() {
        if (script != null) {
            config.script(context.scriptService().search(context.lookup(), scriptLang, script, scriptParams));
        }

        if (!assumeUnique) {
            config.ensureUnique(true);
        }

        if (!assumeSorted && requiresSortedValues) {
            config.ensureSorted(true);
        }

        if (field == null) {
            return config;
        }

        FieldMapper<?> mapper = context.smartNameFieldMapper(field);
        if (mapper == null) {
            config.unmapped(true);
            return config;
        }

        IndexFieldData<?> indexFieldData = context.fieldData().getForField(mapper);
        config.fieldContext(new FieldContext(field, indexFieldData));
        return config;
    }
}
