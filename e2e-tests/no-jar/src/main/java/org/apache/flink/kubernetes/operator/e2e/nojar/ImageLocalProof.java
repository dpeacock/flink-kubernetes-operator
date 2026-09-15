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

package org.apache.flink.kubernetes.operator.e2e.nojar;

import org.apache.flink.kubernetes.operator.e2e.nojar.helper.DeterministicHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Path;

/** Fails rather than treating a BLOB-cache copy as evidence of image-local class loading. */
final class ImageLocalProof {
    private static final Logger LOG = LoggerFactory.getLogger(ImageLocalProof.class);
    private static final String APP_JAR = "/opt/flink/lib/no-jar-test.jar";
    private static final String HELPER_JAR = "/opt/flink/lib/no-jar-test-helper.jar";

    private ImageLocalProof() {}

    static void verify(String role) throws Exception {
        final URL app = NoJarTestJob.class.getProtectionDomain().getCodeSource().getLocation();
        final URL process =
                NoJarTestJob.CounterProcess.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation();
        final URL helper =
                DeterministicHelper.class.getProtectionDomain().getCodeSource().getLocation();
        final URL resource = DeterministicHelper.resource();
        requireFile(app, APP_JAR);
        requireFile(process, APP_JAR);
        requireFile(helper, HELPER_JAR);
        if (!(resource.openConnection() instanceof JarURLConnection connection)) {
            throw new IllegalStateException("Helper resource must come from its image-local JAR");
        }
        requireFile(connection.getJarFileURL(), HELPER_JAR);
        LOG.info(
                "IMAGE_LOCAL role={} app={} process={} helper={} resource={}",
                role,
                app,
                process,
                helper,
                resource);
    }

    static void requireFile(URL actual, String expected) throws Exception {
        if (!"file".equals(actual.getProtocol())
                || !Path.of(actual.toURI()).toRealPath().equals(Path.of(expected).toRealPath())) {
            throw new IllegalStateException(
                    "Expected image-local " + expected + ", found " + actual);
        }
    }
}
