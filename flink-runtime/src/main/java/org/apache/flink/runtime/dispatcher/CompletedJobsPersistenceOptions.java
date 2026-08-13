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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.JobManagerOptions;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Options for the durable completed-jobs store (see {@link DurableExecutionGraphInfoPersister}).
 *
 * <p>These deliberately live in {@code flink-runtime} rather than next to their siblings in {@link
 * JobManagerOptions}: this feature is shipped as a patched {@code flink-runtime} jar that is
 * prepended to an otherwise stock Flink distribution. {@code JobManagerOptions} is loaded from the
 * stock {@code flink-dist} jar, so a field added there would not exist at runtime and any reference
 * to it would fail with {@code NoSuchFieldError} during JobManager startup.
 */
@Internal
public class CompletedJobsPersistenceOptions {

    private CompletedJobsPersistenceOptions() {}

    /**
     * Durable directory where the JobManager persists the execution graphs of globally-terminated
     * jobs so that they survive JobManager failures.
     */
    public static final ConfigOption<String> COMPLETED_JOBS_PERSIST_DIR =
            key("jobmanager.completed-jobs.persist-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Durable directory (e.g. on S3/HDFS) where the JobManager persists the execution graph of "
                                    + "every globally-terminated job that has checkpoint statistics, before the job is "
                                    + "registered as terminated in the JobResultStore. On startup the JobManager "
                                    + "re-populates its completed-jobs store from this directory, so finished jobs "
                                    + "(including their checkpoint history and final savepoint path) remain visible via "
                                    + "the REST API and web UI across JobManager failures. The effective directory is "
                                    + "'<value>/<high-availability.cluster-id>'. If not set, the feature is disabled.");

    /** Retention for entries persisted under {@link #COMPLETED_JOBS_PERSIST_DIR}. */
    public static final ConfigOption<Duration> COMPLETED_JOBS_PERSIST_RETENTION =
            key("jobmanager.completed-jobs.persist-retention")
                    .durationType()
                    .defaultValue(Duration.ofHours(72))
                    .withDescription(
                            "Retention for entries under '"
                                    + COMPLETED_JOBS_PERSIST_DIR.key()
                                    + "'. Files older than this are deleted when a JobManager starts. Entries are "
                                    + "deliberately not deleted when a job's cleanup completes, so that repeated "
                                    + "JobManager failures shortly after job termination cannot lose the record.");
}
