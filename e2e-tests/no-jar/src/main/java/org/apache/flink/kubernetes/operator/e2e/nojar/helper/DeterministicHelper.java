/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.e2e.nojar.helper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/** A separate image-local dependency, intentionally excluded from the application JAR. */
public final class DeterministicHelper {
    private final long multiplier;
    private final long offset;

    public DeterministicHelper() throws IOException {
        final Properties properties = new Properties();
        try (InputStream stream = resource().openStream()) {
            properties.load(stream);
        }
        if (!"no-jar-helper-v1".equals(properties.getProperty("marker"))) {
            throw new IllegalStateException("Wrong helper resource");
        }
        multiplier = Long.parseLong(properties.getProperty("multiplier"));
        offset = Long.parseLong(properties.getProperty("offset"));
        if (multiplier != 17 || offset != 7) {
            throw new IllegalStateException("Wrong helper arithmetic configuration");
        }
    }

    public long transform(long n) {
        return Math.addExact(Math.multiplyExact(n, multiplier), offset);
    }

    public static URL resource() {
        final URL resource = DeterministicHelper.class.getResource("/no-jar-helper.properties");
        if (resource == null) {
            throw new IllegalStateException("Missing image-local helper resource");
        }
        return resource;
    }
}
