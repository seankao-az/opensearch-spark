/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.flint.core.metadata.log.OptimisticTransaction;

public interface FlintMetadataLogService {

  /**
   * Start a new optimistic transaction.
   *
   * @param indexName index name
   * @param dataSourceName TODO: read from elsewhere in future
   * @return transaction handle
   */
  <T> OptimisticTransaction<T> startTransaction(String indexName, String dataSourceName);

  /**
   *
   * Start a new optimistic transaction.
   *
   * @param indexName index name
   * @param dataSourceName TODO: read from elsewhere in future
   * @param forceInit forceInit create empty translog if not exist.
   * @return transaction handle
   */
  <T> OptimisticTransaction<T> startTransaction(String indexName, String dataSourceName,
                                                boolean forceInit);

  /**
   * Create {@link Object}.
   * @return {@link Object}
   */
  Object createClient();
}
