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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public abstract class XContentPushParser {

    public static interface Callback {

        void on(XContentParser.Token token, XContentParser parser) throws IOException;

    }

    public static class AbstractCallback implements Callback {

        protected String fieldName;
        protected final List<String> objects = new ArrayList<>(10);
        protected final List<String> arrays = new ArrayList<>(10);

        @Override
        public void on(XContentParser.Token token, XContentParser parser) throws IOException {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                return;
            }
            if (token == XContentParser.Token.START_OBJECT) {
                objects.add(fieldName);
                onObjectStart(fieldName, token, parser);
                return;
            }
            if (token == XContentParser.Token.END_OBJECT) {
                fieldName = objects.remove(objects.size() - 1);
                onObjectEnd(fieldName, token, parser);
                return;
            }
            if (token == XContentParser.Token.START_ARRAY) {
                arrays.add(fieldName);
                onArrayStart(fieldName, token, parser);
                fieldName = null;
                return;
            }
            if (token == XContentParser.Token.END_ARRAY) {
                fieldName = arrays.remove(arrays.size() - 1);
                onArrayEnd(fieldName, token, parser);
                return;
            }
            // must be a value field
            onValue(fieldName, token, parser);
        }

        void onObjectStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {}

        void onObjectEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {}

        void onArrayStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {}

        void onArrayEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {}

        void onValue(String name, XContentParser.Token token, XContentParser parser) throws IOException {}
    }

    protected final Consumer consumer;


    private XContentPushParser(Callback[] callbacks) {
        this.consumer = new Consumer(callbacks);
    }

    public abstract void parse(XContentParser parser) throws IOException;

    public static Builder object() {
        return new Object.Builder();
    }

    public static Builder array() {
        return new Array.Builder();
    }

    public static abstract class Builder<P extends XContentPushParser, B extends Builder> {

        private final List<Callback> callbacks = new ArrayList<Callback>();

        private Builder() {}

        public B add(Callback callback) {
            callbacks.add(callback);
            return (B) this;
        }

        public final P build() {
            return build(callbacks.toArray(new Callback[callbacks.size()]));
        }

        protected abstract P build(Callback[] callbacks);

    }

    private static class Object extends XContentPushParser {

        private Object(Callback[] callbacks) {
            super(callbacks);
        }

        public void parse(XContentParser parser) throws IOException {
            XContentParser.Token token;
            do {
                token = parser.nextToken();
                consumer.on(token, parser);
            } while(!consumer.consumed());
        }

        private static class Builder extends XContentPushParser.Builder<Object, Builder> {

            @Override
            protected Object build(Callback[] callbacks) {
                return new Object(callbacks);
            }
        }
    }

    private static class Array extends XContentPushParser {

        private Array(Callback[] callbacks) {
            super(callbacks);
        }

        public void parse(XContentParser parser) throws IOException {
            XContentParser.Token token;
            do {
                token = parser.nextToken();
                consumer.on(token, parser);
            } while (!consumer.consumed());
        }

        private static class Builder extends XContentPushParser.Builder<Array, Builder> {

            @Override
            protected Array build(Callback[] callbacks) {
                return new Array(callbacks);
            }
        }
    }

    private static class Consumer extends AbstractCallback {

        protected final Callback[] callbacks;

        private Consumer(Callback[] callbacks) {
            this.callbacks = callbacks;
        }

        @Override
        public void on(XContentParser.Token token, XContentParser parser) throws IOException {
            super.on(token, parser);
            for (int i = 0; i < callbacks.length; i++) {
                callbacks[i].on(token, parser);
            }
        }

        private boolean consumed() {
            return objects.size() == 0 && arrays.size() == 0;
        }
    }

//
//    public static void main(String[] args) throws Exception {
//
//        TokenCallback p1 = new AbstractTokenCallback() {
//
//            int indent = 0;
//
//            String indent(String text) {
//                StringBuilder sb = new StringBuilder();
//                for (int i = 0; i < indent; i++) {
//                    sb.append("\t");
//                }
//                return sb.append(text).toString();
//            }
//
//            @Override
//            void onObjectStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {
//                System.out.println(indent("Start Object: " + name));
//                indent++;
//            }
//
//            @Override
//            void onObjectEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {
//                indent--;
//                System.out.println(indent("End Object: " + name));
//            }
//
//            @Override
//            void onArrayStart(String name, XContentParser.Token token, XContentParser parser) throws IOException {
//                System.out.println(indent("Start Array: " + name));
//                indent++;
//            }
//
//            @Override
//            void onArrayEnd(String name, XContentParser.Token token, XContentParser parser) throws IOException {
//                indent--;
//                System.out.println(indent("End Array: " + name));
//            }
//
//            @Override
//            void onValue(String name, XContentParser.Token token, XContentParser parser) throws IOException {
//                System.out.println(indent(name + " = " + parser.text()));
//            }
//        };
//        InputStream in = null;
//        try {
//            in = XContentPushParser.class.getResourceAsStream("/state.json");
//            XContentParser xcp = new JsonXContentParser(new JsonFactory().createParser(in));
//            XContentPushParser parser = XContentPushParser.object().add(p1).build();
//            parser.parse(xcp);
//
//        } finally {
//            in.close();
//        }
//
//    }

}
