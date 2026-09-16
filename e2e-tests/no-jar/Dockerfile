# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Build the two fixture JARs with Java 17: mvn -f e2e-tests/no-jar/pom.xml clean verify
# The build context is e2e-tests/no-jar. The same image supports both submission modes.
FROM flink:2.3.0-scala_2.12-java17@sha256:658a4f50e1eaed2bdb6cc70d84afd5f8eb5c822be82aa077185f470b38dd3fde
COPY --chown=flink:flink target/no-jar-test.jar /opt/flink/lib/no-jar-test.jar
COPY --chown=flink:flink target/no-jar-test-helper.jar /opt/flink/lib/no-jar-test-helper.jar
