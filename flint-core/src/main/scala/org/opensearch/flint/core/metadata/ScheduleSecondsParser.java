/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metadata;

import org.apache.parquet.Strings;
import org.apache.spark.sql.execution.streaming.Triggers;

/**
 * Utility class for parsing schedules.
 */
public class ScheduleSecondsParser {

    /**
     * Parses a schedule string into an integer.
     *
     * @param scheduleStr the schedule string to parse
     * @return the parsed integer
     * @throws IllegalArgumentException if the schedule string is invalid
     */
    public static int parse(String scheduleStr) {
        if (Strings.isNullOrEmpty(scheduleStr)) {
            throw new IllegalArgumentException("Schedule string must not be null or empty.");
        }

        Long millis = Triggers.convert(scheduleStr);

        // Convert milliseconds to seconds (rounding down)
        return (int) (millis / 1000);
    }
}