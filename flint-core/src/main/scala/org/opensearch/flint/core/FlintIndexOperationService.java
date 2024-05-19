/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.util.List;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.flint.core.storage.FlintReader;
import org.opensearch.flint.core.storage.FlintWriter;

public interface FlintIndexOperationService {

  /**
   * Create a Flint index with the metadata given.
   *
   * @param indexName index name
   * @param metadata  index metadata
   */
  void createIndex(String indexName, FlintMetadata metadata);

  /**
   * Does Flint index with the given name exist
   *
   * @param indexName index name
   * @return true if the index exists, otherwise false
   */
  boolean exists(String indexName);

  /**
   * Update a Flint index with the metadata given.
   *
   * @param indexName index name
   * @param metadata  index metadata
   */
  void updateIndex(String indexName, FlintMetadata metadata);

  /**
   * Delete a Flint index.
   *
   * @param indexName index name
   */
  void deleteIndex(String indexName);

  // TODO: better return type
  String[] getIndex(String indexName);
  List<String[]> getAllIndex(String indexNamePattern);

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all
   * @return {@link FlintReader}.
   */
  FlintReader createReader(String indexName, String query);

  /**
   * Create {@link FlintWriter}.
   *
   * @param indexName - index name
   * @return {@link FlintWriter}
   */
  FlintWriter createWriter(String indexName);

  /**
   * Create {@link Object}.
   * @return {@link Object}
   */
  Object createClient();
}
