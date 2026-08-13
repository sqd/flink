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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * Persists the {@link ExecutionGraphInfo} of globally-terminated jobs to a durable directory (see
 * {@link CompletedJobsPersistenceOptions#COMPLETED_JOBS_PERSIST_DIR}) so that completed jobs
 * survive JobManager failures.
 *
 * <p>The {@link Dispatcher} writes an entry <em>before</em> the job is registered as terminated in
 * the {@code JobResultStore} and re-populates its {@code ExecutionGraphInfoStore} from this
 * directory on startup. This guarantees that a job that is durably marked terminal always has
 * recoverable details (in particular the checkpoint history with the final savepoint path of a
 * stop-with-savepoint operation) served via the REST API after a JobManager failover or restart.
 *
 * <p><strong>Safety invariant:</strong> an entry is only persisted/hydrated if it carries a
 * non-empty {@link CheckpointStatsSnapshot} with a restorable checkpoint pointer (see {@link
 * #isEligible(ExecutionGraphInfo)}). Serving a terminal job with <em>empty</em> checkpoint
 * statistics is dangerous for external tooling: the Flink Kubernetes Operator interprets "terminal
 * job, no savepoint/checkpoint recorded" as a stateless job and may redeploy without state. A
 * missing entry (REST "job not found") is always safer than an empty one.
 */
class DurableExecutionGraphInfoPersister {

    private static final Logger LOG =
            LoggerFactory.getLogger(DurableExecutionGraphInfoPersister.class);

    private static final String TMP_FILE_SUFFIX = ".tmp";

    private static final int MAX_WRITE_ATTEMPTS = 3;
    private static final long WRITE_RETRY_BACKOFF_MS = 500L;

    private final Path storageDir;
    private final Duration retention;

    private DurableExecutionGraphInfoPersister(Path storageDir, Duration retention) {
        this.storageDir = Preconditions.checkNotNull(storageDir);
        this.retention = Preconditions.checkNotNull(retention);
    }

    /**
     * Creates a persister from the configuration, or {@link Optional#empty()} if {@link
     * CompletedJobsPersistenceOptions#COMPLETED_JOBS_PERSIST_DIR} is not set. The effective
     * directory is scoped by the HA cluster-id so that multiple clusters can share a base
     * directory.
     */
    static Optional<DurableExecutionGraphInfoPersister> fromConfiguration(
            Configuration configuration) {
        final String baseDir =
                configuration.get(CompletedJobsPersistenceOptions.COMPLETED_JOBS_PERSIST_DIR);
        if (baseDir == null) {
            return Optional.empty();
        }
        final String clusterId = configuration.get(HighAvailabilityOptions.HA_CLUSTER_ID);
        return Optional.of(
                new DurableExecutionGraphInfoPersister(
                        new Path(baseDir, clusterId),
                        configuration.get(
                                CompletedJobsPersistenceOptions.COMPLETED_JOBS_PERSIST_RETENTION)));
    }

    @Override
    public String toString() {
        return String.format(
                "DurableExecutionGraphInfoPersister{storageDir=%s, retention=%s}",
                storageDir, retention);
    }

    /**
     * Whether persisting/hydrating the given graph is safe: the job must be globally terminal and
     * its checkpoint statistics must contain at least one restorable pointer (a completed
     * checkpoint, a savepoint, or a restored checkpoint). See the class-level safety invariant.
     */
    static boolean isEligible(ExecutionGraphInfo executionGraphInfo) {
        final ArchivedExecutionGraph graph = executionGraphInfo.getArchivedExecutionGraph();
        if (!graph.getState().isGloballyTerminalState()) {
            return false;
        }
        final CheckpointStatsSnapshot stats = graph.getCheckpointStatsSnapshot();
        return stats != null
                && (stats.getHistory().getLatestCompletedCheckpoint() != null
                        || stats.getHistory().getLatestSavepoint() != null
                        || stats.getLatestRestoredCheckpoint() != null);
    }

    /**
     * Persists the given graph on the given executor. The write is published atomically (temp file
     * + rename) so that a reader can never observe a truncated entry. The returned future fails if
     * the entry could not be persisted after retries; callers are expected to fail closed in that
     * case.
     */
    CompletableFuture<Void> persistAsync(ExecutionGraphInfo executionGraphInfo, Executor executor) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        persistWithRetry(executionGraphInfo);
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                },
                executor);
    }

    private void persistWithRetry(ExecutionGraphInfo executionGraphInfo) throws IOException {
        IOException lastFailure = null;
        for (int attempt = 1; attempt <= MAX_WRITE_ATTEMPTS; attempt++) {
            try {
                persist(executionGraphInfo);
                return;
            } catch (IOException e) {
                lastFailure = e;
                LOG.warn(
                        "Could not persist execution graph of job {} to {} (attempt {}/{}).",
                        executionGraphInfo.getJobId(),
                        storageDir,
                        attempt,
                        MAX_WRITE_ATTEMPTS,
                        e);
                if (attempt < MAX_WRITE_ATTEMPTS) {
                    try {
                        Thread.sleep(WRITE_RETRY_BACKOFF_MS * attempt);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw lastFailure;
    }

    private void persist(ExecutionGraphInfo executionGraphInfo) throws IOException {
        final FileSystem fileSystem = storageDir.getFileSystem();
        fileSystem.mkdirs(storageDir);

        final String fileName = executionGraphInfo.getJobId().toString();
        final Path finalPath = new Path(storageDir, fileName);
        final Path tmpPath = new Path(storageDir, fileName + TMP_FILE_SUFFIX);

        try (FSDataOutputStream out = fileSystem.create(tmpPath, FileSystem.WriteMode.OVERWRITE)) {
            InstantiationUtil.serializeObject(out, executionGraphInfo);
        }

        // a pre-existing entry can only stem from a previous execution of a re-used job id;
        // the new terminal state supersedes it
        if (fileSystem.exists(finalPath)) {
            fileSystem.delete(finalPath, false);
        }
        if (!fileSystem.rename(tmpPath, finalPath)) {
            throw new IOException(
                    String.format(
                            "Could not publish execution graph file %s (rename from %s failed).",
                            finalPath, tmpPath));
        }
        LOG.info(
                "Durably persisted execution graph of globally terminated job {} to {}.",
                executionGraphInfo.getJobId(),
                finalPath);
    }

    /**
     * Loads all eligible entries from the storage directory, deleting entries older than the
     * configured retention as well as leftover temp files. Unreadable entries (truncated files or
     * entries written by a different Flink version) are skipped. This method never throws; on
     * unexpected errors it returns what could be read so a JobManager start is never blocked.
     */
    Collection<ExecutionGraphInfo> loadAll() {
        final List<ExecutionGraphInfo> result = new ArrayList<>();
        try {
            final FileSystem fileSystem = storageDir.getFileSystem();
            if (!fileSystem.exists(storageDir)) {
                return result;
            }
            final FileStatus[] entries = fileSystem.listStatus(storageDir);
            if (entries == null) {
                return result;
            }
            final long expiredBefore = System.currentTimeMillis() - retention.toMillis();
            for (FileStatus entry : entries) {
                if (entry.isDir()) {
                    continue;
                }
                final Path path = entry.getPath();
                if (path.getName().endsWith(TMP_FILE_SUFFIX)
                        || entry.getModificationTime() < expiredBefore) {
                    tryDelete(fileSystem, path);
                    continue;
                }
                loadEntry(fileSystem, path).ifPresent(result::add);
            }
        } catch (Exception e) {
            LOG.warn(
                    "Could not fully load persisted completed jobs from {}; continuing with {} entries.",
                    storageDir,
                    result.size(),
                    e);
        }
        return result;
    }

    private Optional<ExecutionGraphInfo> loadEntry(FileSystem fileSystem, Path path) {
        final ExecutionGraphInfo executionGraphInfo;
        try (FSDataInputStream in = fileSystem.open(path)) {
            executionGraphInfo =
                    InstantiationUtil.deserializeObject(in, getClass().getClassLoader());
        } catch (Exception e) {
            // e.g. a file written by a different Flink version; retention will reap it
            LOG.warn("Skipping unreadable completed-job entry {}.", path, e);
            return Optional.empty();
        }
        if (!isEligible(executionGraphInfo)) {
            LOG.warn(
                    "Skipping persisted completed-job entry {} without restorable checkpoint statistics.",
                    path);
            return Optional.empty();
        }
        return Optional.of(executionGraphInfo);
    }

    private static void tryDelete(FileSystem fileSystem, Path path) {
        try {
            fileSystem.delete(path, false);
            LOG.debug("Deleted expired or leftover completed-job entry {}.", path);
        } catch (IOException e) {
            LOG.warn("Could not delete expired completed-job entry {}.", path, e);
        }
    }
}
