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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.TestingCheckpointStatsSnapshots;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DurableExecutionGraphInfoPersister}. */
class DurableExecutionGraphInfoPersisterTest {

    private static final String CLUSTER_ID = "test-cluster";
    private static final String SAVEPOINT_PATH = "s3://savepoints/savepoint-abc123-def456";

    @TempDir private Path temporaryFolder;

    @Test
    void testDisabledWhenDirectoryNotConfigured() {
        assertThat(DurableExecutionGraphInfoPersister.fromConfiguration(new Configuration()))
                .isEmpty();
    }

    @Test
    void testPersistAndLoadRoundTrip() {
        final DurableExecutionGraphInfoPersister persister = createPersister(null);
        final JobID jobId = new JobID();

        persister.persistAsync(createEligibleGraphInfo(jobId), Runnable::run).join();

        final Collection<ExecutionGraphInfo> loaded = persister.loadAll();
        assertThat(loaded).hasSize(1);
        final ExecutionGraphInfo loadedInfo = loaded.iterator().next();
        final ArchivedExecutionGraph graph = loadedInfo.getArchivedExecutionGraph();
        assertThat(loadedInfo.getJobId()).isEqualTo(jobId);
        assertThat(graph.getState()).isEqualTo(JobStatus.FINISHED);
        assertThat(
                        graph.getCheckpointStatsSnapshot()
                                .getHistory()
                                .getLatestSavepoint()
                                .getExternalPath())
                .isEqualTo(SAVEPOINT_PATH);
    }

    @Test
    void testPersistIsScopedByClusterId() {
        final DurableExecutionGraphInfoPersister persister = createPersister(null);
        persister.persistAsync(createEligibleGraphInfo(new JobID()), Runnable::run).join();

        assertThat(temporaryFolder.resolve(CLUSTER_ID)).isDirectory();
    }

    @Test
    void testPersistOverwritesPreviousEntryForSameJob() {
        final DurableExecutionGraphInfoPersister persister = createPersister(null);
        final JobID jobId = new JobID();

        persister
                .persistAsync(
                        createGraphInfo(
                                jobId,
                                JobStatus.FINISHED,
                                TestingCheckpointStatsSnapshots.withCompletedSavepoint(
                                        1, "s3://savepoints/old")),
                        Runnable::run)
                .join();
        persister.persistAsync(createEligibleGraphInfo(jobId), Runnable::run).join();

        final Collection<ExecutionGraphInfo> loaded = persister.loadAll();
        assertThat(loaded).hasSize(1);
        assertThat(
                        loaded.iterator()
                                .next()
                                .getArchivedExecutionGraph()
                                .getCheckpointStatsSnapshot()
                                .getHistory()
                                .getLatestSavepoint()
                                .getExternalPath())
                .isEqualTo(SAVEPOINT_PATH);
    }

    @Test
    void testLoadSkipsUnreadableAndLeftoverTmpFiles() throws IOException {
        final DurableExecutionGraphInfoPersister persister = createPersister(null);
        persister.persistAsync(createEligibleGraphInfo(new JobID()), Runnable::run).join();

        final Path storageDir = temporaryFolder.resolve(CLUSTER_ID);
        final Path corruptFile = storageDir.resolve(new JobID().toString());
        Files.write(
                corruptFile, "not a serialized execution graph".getBytes(StandardCharsets.UTF_8));
        final Path leftoverTmpFile = storageDir.resolve(new JobID() + ".tmp");
        Files.write(leftoverTmpFile, new byte[] {1, 2, 3});

        assertThat(persister.loadAll()).hasSize(1);
        // the corrupt entry is kept for the retention sweep, the tmp file is deleted
        assertThat(corruptFile).exists();
        assertThat(leftoverTmpFile).doesNotExist();
    }

    @Test
    void testLoadDeletesExpiredEntries() throws Exception {
        final DurableExecutionGraphInfoPersister persister = createPersister(Duration.ofMillis(1));
        final JobID jobId = new JobID();
        persister.persistAsync(createEligibleGraphInfo(jobId), Runnable::run).join();

        Thread.sleep(10);

        assertThat(persister.loadAll()).isEmpty();
        assertThat(temporaryFolder.resolve(CLUSTER_ID).resolve(jobId.toString())).doesNotExist();
    }

    @Test
    void testEligibility() {
        final CheckpointStatsSnapshot statsWithSavepoint =
                TestingCheckpointStatsSnapshots.withCompletedSavepoint(1, SAVEPOINT_PATH);

        assertThat(
                        DurableExecutionGraphInfoPersister.isEligible(
                                createGraphInfo(
                                        new JobID(), JobStatus.FINISHED, statsWithSavepoint)))
                .isTrue();
        assertThat(
                        DurableExecutionGraphInfoPersister.isEligible(
                                createGraphInfo(
                                        new JobID(), JobStatus.CANCELED, statsWithSavepoint)))
                .isTrue();
        // non-globally-terminal states must never be persisted
        assertThat(
                        DurableExecutionGraphInfoPersister.isEligible(
                                createGraphInfo(
                                        new JobID(), JobStatus.RUNNING, statsWithSavepoint)))
                .isFalse();
        assertThat(
                        DurableExecutionGraphInfoPersister.isEligible(
                                createGraphInfo(
                                        new JobID(), JobStatus.SUSPENDED, statsWithSavepoint)))
                .isFalse();
        // never persist/serve empty checkpoint statistics (operator safety invariant)
        assertThat(
                        DurableExecutionGraphInfoPersister.isEligible(
                                createGraphInfo(new JobID(), JobStatus.FINISHED, null)))
                .isFalse();
        assertThat(
                        DurableExecutionGraphInfoPersister.isEligible(
                                createGraphInfo(
                                        new JobID(),
                                        JobStatus.FINISHED,
                                        TestingCheckpointStatsSnapshots
                                                .withoutRestorablePointer())))
                .isFalse();
    }

    @Test
    void testLoadSkipsIneligibleEntries() {
        final DurableExecutionGraphInfoPersister persister = createPersister(null);
        // the Dispatcher guards persistence with isEligible(); simulate a bogus entry written
        // by a buggy/older version by persisting an ineligible graph directly
        persister
                .persistAsync(
                        createGraphInfo(
                                new JobID(),
                                JobStatus.FINISHED,
                                TestingCheckpointStatsSnapshots.withoutRestorablePointer()),
                        Runnable::run)
                .join();

        assertThat(persister.loadAll()).isEmpty();
    }

    private DurableExecutionGraphInfoPersister createPersister(@Nullable Duration retention) {
        final Configuration configuration = new Configuration();
        configuration.set(
                CompletedJobsPersistenceOptions.COMPLETED_JOBS_PERSIST_DIR,
                temporaryFolder.toUri().toString());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, CLUSTER_ID);
        if (retention != null) {
            configuration.set(
                    CompletedJobsPersistenceOptions.COMPLETED_JOBS_PERSIST_RETENTION, retention);
        }
        return DurableExecutionGraphInfoPersister.fromConfiguration(configuration)
                .orElseThrow(IllegalStateException::new);
    }

    private static ExecutionGraphInfo createEligibleGraphInfo(JobID jobId) {
        return createGraphInfo(
                jobId,
                JobStatus.FINISHED,
                TestingCheckpointStatsSnapshots.withCompletedSavepoint(17, SAVEPOINT_PATH));
    }

    private static ExecutionGraphInfo createGraphInfo(
            JobID jobId, JobStatus state, @Nullable CheckpointStatsSnapshot statsSnapshot) {
        return new ExecutionGraphInfo(
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(state)
                        .setCheckpointStatsSnapshot(statsSnapshot)
                        .build());
    }
}
