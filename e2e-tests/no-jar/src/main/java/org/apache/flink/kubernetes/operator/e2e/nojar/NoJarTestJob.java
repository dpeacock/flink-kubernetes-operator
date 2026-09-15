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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.kubernetes.operator.e2e.nojar.helper.DeterministicHelper;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Image-local Flink 2.3 fixture for both absent jarURI and local:///opt/flink/lib/no-jar-test.jar.
 *
 * <p>Arguments: --interval-ms (default 100), --checkpoint-interval-ms (default 1000), and
 * --require-restored (default false). Use parallelism one and strict restore configuration. Results
 * satisfy value = 17*n + 7; checkpoint rollback can replay results, so logs are not an exactly-once
 * sink. Within each execution attempt, n must be consecutive and strictly increasing.
 */
public final class NoJarTestJob {
    private static final Logger LOG = LoggerFactory.getLogger(NoJarTestJob.class);

    private NoJarTestJob() {}

    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final long interval = parameters.getLong("interval-ms", 100);
        final long checkpointInterval = parameters.getLong("checkpoint-interval-ms", 1000);
        final boolean requireRestored = parameters.getBoolean("require-restored", false);
        if (interval <= 0 || checkpointInterval <= 0) {
            throw new IllegalArgumentException("Intervals must be positive");
        }
        for (String key : parameters.toMap().keySet()) {
            if (!List.of("interval-ms", "checkpoint-interval-ms", "require-restored")
                    .contains(key)) {
                throw new IllegalArgumentException("Unknown argument: " + key);
            }
        }
        ImageLocalProof.verify("jobmanager");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        verifyRestoreSettings(env.getConfiguration());
        LOG.info(
                "FIXTURE_CONFIG intervalMs={} checkpointIntervalMs={} requireRestored={} ignoreUnclaimedState=false",
                interval,
                checkpointInterval,
                requireRestored);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.setMaxParallelism(1);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.fromSequence(1, Long.MAX_VALUE)
                .name("checkpointed-sequence")
                .uid("no-jar-sequence-v1")
                .process(new CounterProcess(interval, requireRestored))
                .name("verified-counter")
                .uid("no-jar-counter-v1")
                .sinkTo(new DiscardingSink<>())
                .name("discard-after-log")
                .uid("no-jar-sink-v1");
        final StreamGraph graph = env.getStreamGraph();
        graph.setJobName("no-jar-stateful-fixture");
        env.registerJobListener(new SubmissionEvidence(graph));
        env.execute(graph);
    }

    /** Real worker function with independently checked source and operator checkpoint positions. */
    public static final class CounterProcess extends ProcessFunction<Long, Long>
            implements CheckpointedFunction, CheckpointListener {
        private static final long serialVersionUID = 1L;
        private final long intervalMillis;
        private final boolean requireRestored;
        private transient ListState<Long> counterState;
        private transient DeterministicHelper helper;
        private long count;
        private boolean restored;
        private long restoredCheckpoint;

        public CounterProcess(long intervalMillis, boolean requireRestored) {
            this.intervalMillis = intervalMillis;
            this.requireRestored = requireRestored;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            restored = context.isRestored();
            restoredCheckpoint = context.getRestoredCheckpointId().orElse(-1);
            counterState =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("monotonic-counter-v1", Long.class));
            count = restoreCount(counterState.get(), restored, requireRestored);
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            ImageLocalProof.verify("taskmanager");
            helper = new DeterministicHelper();
            LOG.info(
                    "STATE_INIT restored={} n={} checkpoint={} subtask={} attempt={}",
                    restored,
                    count,
                    restoredCheckpoint,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                    getRuntimeContext().getTaskInfo().getAttemptNumber());
        }

        @Override
        public void processElement(Long n, Context context, Collector<Long> out) throws Exception {
            if (n != Math.addExact(count, 1)) {
                throw new IllegalStateException(
                        "Non-consecutive source: count=" + count + ", n=" + n);
            }
            final long value = helper.transform(n);
            count = n;
            LOG.info(
                    "RESULT n={} value={} restored={} attempt={}",
                    n,
                    value,
                    restored,
                    getRuntimeContext().getTaskInfo().getAttemptNumber());
            out.collect(value);
            // A bounded pause in the mailbox keeps output readable while allowing barrier progress.
            Thread.sleep(intervalMillis);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            counterState.update(List.of(count));
            LOG.info(
                    "STATE_SNAPSHOT checkpoint={} n={} restored={}",
                    context.getCheckpointId(),
                    count,
                    restored);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            LOG.info("CHECKPOINT_COMPLETE checkpoint={} restored={}", checkpointId, restored);
        }
    }

    static void verifyRestoreSettings(ReadableConfig configuration) {
        if (configuration.get(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE)) {
            throw new IllegalArgumentException(
                    "execution.state-recovery.ignore-unclaimed-state must be false");
        }
    }

    static long restoreCount(Iterable<Long> entries, boolean restored, boolean requireRestored) {
        if (requireRestored && !restored) {
            throw new IllegalStateException("A checkpoint/savepoint restore is required");
        }
        final Iterator<Long> iterator = entries.iterator();
        if (!restored) {
            if (iterator.hasNext()) {
                throw new IllegalStateException("Unexpected state on a fresh start");
            }
            return 0;
        }
        if (!iterator.hasNext()) {
            throw new IllegalStateException("Restored checkpoint is missing monotonic-counter-v1");
        }
        final Long count = iterator.next();
        if (count == null || count < 0 || iterator.hasNext()) {
            throw new IllegalStateException("Expected exactly one nonnegative restored counter");
        }
        return count;
    }
}
