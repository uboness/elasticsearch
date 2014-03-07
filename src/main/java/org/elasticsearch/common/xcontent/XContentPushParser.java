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

package org.elasticsearch.common.xcontent;

import com.google.common.base.Objects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class XContentPushParser<T> {

    public static interface Callback<T> {

        boolean on(XContentParser.Token token, XContentParser parser) throws IOException;

        T process();

    }

    public static abstract class AbstractCallback<T> implements Callback<T> {

        protected String fieldName;
        protected final List<String> objects = new ArrayList<>(10);
        protected final List<String> arrays = new ArrayList<>(10);

        @Override
        public boolean on(XContentParser.Token token, XContentParser parser) throws IOException {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                return true;
            }
            if (token == XContentParser.Token.START_OBJECT) {
                boolean consumed = onObjectStart(fieldName, token, parser);
                objects.add(fieldName);
                return consumed;
            }
            if (token == XContentParser.Token.END_OBJECT) {
                if (!objects.isEmpty()) {
                    fieldName = objects.remove(objects.size() - 1);
                    return onObjectEnd(fieldName, token, parser);
                }
                return false;
            }
            if (token == XContentParser.Token.START_ARRAY) {
                boolean consumed = onArrayStart(fieldName, token, parser);
                arrays.add(fieldName);
                fieldName = null;
                return consumed;
            }
            if (token == XContentParser.Token.END_ARRAY) {
                if (!arrays.isEmpty()) {
                    fieldName = arrays.remove(arrays.size() - 1);
                    return onArrayEnd(fieldName, token, parser);
                }
                return false;
            }
            // must be a value field
            return onValue(fieldName, token, parser);
        }

        protected boolean currentRoot() {
            return objects.size() == 0 && arrays.size() == 0;
        }

        protected boolean currentObject(String name) {
            if (objects.isEmpty()) {
                return false;
            }
            return Objects.equal(objects.get(objects.size() - 1), name);
        }

        protected boolean currentArray(String name) {
            if (arrays.isEmpty()) {
                return false;
            }
            return Objects.equal(arrays.get(arrays.size() - 1), name);
        }

        protected java.lang.Object value(XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return parser.text();
            }
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return parser.numberValue();
            }
            if (parser.currentToken() == XContentParser.Token.VALUE_BOOLEAN) {
                return parser.booleanValue();
            }
            return null; // null value
        }

        protected boolean isString(XContentParser.Token token) {
            return token == XContentParser.Token.VALUE_STRING;
        }

        protected boolean isBoolean(XContentParser.Token token) {
            return token == XContentParser.Token.VALUE_BOOLEAN;
        }

        protected boolean isNumber(XContentParser.Token token) {
            return token == XContentParser.Token.VALUE_NUMBER;
        }

        protected boolean isNull(XContentParser.Token token) {
            return token == XContentParser.Token.VALUE_NULL;
        }

        protected boolean onObjectStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            return false;
        }

        protected boolean onObjectEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            return false;
        }

        protected boolean onArrayStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            return false;
        }

        protected boolean onArrayEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            return false;
        }

        protected boolean onValue(String name, XContentParser.Token token, XContentParser parser) throws IOException {
            return false;
        }
    }

    protected final Consumer<T> consumer;

    private XContentPushParser(Callback<T> callback) {
        this.consumer = new Consumer<T>(callback);
    }

    public abstract T parse(XContentParser parser) throws IOException;

    public static <T> XContentPushParser<T> object(Callback<T> callback) {
        return new Object<T>(callback);
    }

    public static <T> XContentPushParser<T> array(Callback<T> callback) {
        return new Array<T>(callback);
    }

    private static class Object<T> extends XContentPushParser<T> {

        private Object(Callback<T> callback) {
            super(callback);
        }

        public T parse(XContentParser parser) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT || !consumer.currentRoot()) {
                consumer.on(token, parser);
            }
            return consumer.process();
        }
    }

    private static class Array<T> extends XContentPushParser<T> {

        private Array(Callback<T> callback) {
            super(callback);
        }

        public T parse(XContentParser parser) throws IOException {
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY || !consumer.currentRoot()) {
                consumer.on(token, parser);
            }
            return consumer.process();
        }
    }

    private static class Consumer<T> extends AbstractCallback<T> {

        protected final Callback<T> callback;

        private Consumer(Callback<T> callback) {
            this.callback = callback;
        }

        @Override
        public boolean on(XContentParser.Token token, XContentParser parser) throws IOException {
            super.on(token, parser);
            return callback.on(token, parser);
        }

        @Override
        public T process() {
            return callback.process();
        }
    }
}
