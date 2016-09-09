package com.enow.storm.ActionTopology;

/**
 * Created by writtic on 2016. 9. 9..
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.UnsupportedEncodingException;

import java.util.Collections;
import java.util.List;

import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.nio.ByteBuffer;

public class JsonScheme implements Scheme {
    private static final String UTF8_CHARSET = "UTF-8";
    public static final String JSON_SCHEME_KEY = "json";

    public List<Object> deserialize(ByteBuffer bytes) {
        return new Values(deserializeJson(bytes));
    }

    public List<Object> deserializeJson(ByteBuffer bytes) {
        final String ch;
        try {
            ch = new String(Utils.toByteArray(bytes), UTF8_CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
        final Object json;
        try {
            json = JSONValue.parseWithException(ch);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
        return Collections.singletonList(json);
    }

    public Fields getOutputFields() {
        return new Fields(JSON_SCHEME_KEY);
    }
}