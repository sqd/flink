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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;

/**
 * Test factories for {@link CheckpointStatsSnapshot} instances, usable from tests outside the
 * {@code org.apache.flink.runtime.checkpoint} package (the production constructors are
 * package-private). All returned snapshots are fully serializable.
 */
public class TestingCheckpointStatsSnapshots {

    private TestingCheckpointStatsSnapshots() {}

    /**
     * Creates a snapshot whose history contains a single completed savepoint with the given
     * external path (as produced by e.g. a stop-with-savepoint operation).
     */
    public static CheckpointStatsSnapshot withCompletedSavepoint(
            long checkpointId, String externalPath) {
        final CheckpointProperties props =
                CheckpointProperties.forSavepoint(true, SavepointFormatType.CANONICAL);
        final JobVertexID vertexId = new JobVertexID();

        final CheckpointStatsHistory history = new CheckpointStatsHistory(4);
        history.addInProgressCheckpoint(
                new PendingCheckpointStats(
                        checkpointId, 100L, props, Collections.singletonMap(vertexId, 1)));
        history.replacePendingCheckpointById(
                new CompletedCheckpointStats(
                        checkpointId,
                        100L,
                        props,
                        1,
                        Collections.singletonMap(vertexId, new TaskStateStats(vertexId, 1)),
                        1,
                        42L,
                        0L,
                        0L,
                        false,
                        new SubtaskStateStats(0, 200L),
                        externalPath));

        final CheckpointStatsCounts counts = new CheckpointStatsCounts();
        counts.incrementInProgressCheckpoints();
        counts.incrementCompletedCheckpoints();

        return new CheckpointStatsSnapshot(
                counts,
                new CompletedCheckpointStatsSummary().createSnapshot(),
                history.createSnapshot(),
                null);
    }

    /**
     * Creates a non-null snapshot without any restorable pointer: no completed checkpoint, no
     * savepoint, no restored checkpoint.
     */
    public static CheckpointStatsSnapshot withoutRestorablePointer() {
        return new CheckpointStatsSnapshot(
                new CheckpointStatsCounts(),
                new CompletedCheckpointStatsSummary().createSnapshot(),
                new CheckpointStatsHistory(4).createSnapshot(),
                null);
    }
}
