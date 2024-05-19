/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.opensearch.flint.core.metadata.FlintMetadata;
import org.opensearch.flint.core.metadata.log.FlintMetadataLog;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.metadata.log.OptimisticTransaction;
import org.opensearch.flint.core.storage.FlintReader;
import org.opensearch.flint.core.storage.FlintWriter;

/**
 * Flint client default implementation.
 */
public class FlintClientImpl implements FlintClient {

  private static final Logger LOG = Logger.getLogger(FlintClientImpl.class.getName());

  private final FlintOptions options;
  private final FlintIndexOperationService indexOperationService;
  private final FlintMetadataLogService metadataLogService;

  public FlintClientImpl(FlintOptions options, FlintIndexOperationService indexOperationService, FlintMetadataLogService metadataLogService) {
    this.options = options;
    this.indexOperationService = indexOperationService;
    this.metadataLogService = metadataLogService;
  }

  @Override
  public <T> OptimisticTransaction<T> startTransaction(
      String indexName, String dataSourceName, boolean forceInit) {
    return metadataLogService.startTransaction(indexName, dataSourceName, forceInit);
  }

  @Override
  public <T> OptimisticTransaction<T> startTransaction(String indexName, String dataSourceName) {
    return startTransaction(indexName, dataSourceName, false);
  }

  @Override
  public void createIndex(String indexName, FlintMetadata metadata) {
    indexOperationService.createIndex(indexName, metadata);
  }

  @Override
  public boolean exists(String indexName) {
    return indexOperationService.exists(indexName);
  }

  @Override
  public void updateIndex(String indexName, FlintMetadata metadata) {
    indexOperationService.updateIndex(indexName, metadata);
  }

  @Override
  public void deleteIndex(String indexName) {
    indexOperationService.deleteIndex(indexName);
  }

  @Override
  public FlintReader createReader(String indexName, String query) {
    return indexOperationService.createReader(indexName, query);
  }

  public FlintWriter createWriter(String indexName) {
    return indexOperationService.createWriter(indexName);
  }

  // TODO: remove this function
  @Override
  public IRestHighLevelClient createClient() {
    return (IRestHighLevelClient) indexOperationService.createClient();
  }

  @Override
  public List<FlintMetadata> getAllIndexMetadata(String indexNamePattern) {
    LOG.info("Fetching all Flint index metadata for pattern " + indexNamePattern);
    List<String[]> results = indexOperationService.getAllIndex(indexNamePattern);
    return results.stream()
        .map(result -> constructFlintMetadata(result[0], result[1], result[2]))
        .collect(Collectors.toList());
  }

  @Override
  public FlintMetadata getIndexMetadata(String indexName) {
    LOG.info("Fetching Flint index metadata for " + indexName);
    String[] results = indexOperationService.getIndex(indexName);
    return constructFlintMetadata(indexName, results[0], results[1]);
  }

  /*
   * Constructs Flint metadata with latest metadata log entry attached if it's available.
   * It relies on FlintOptions to provide data source name.
   */
  private FlintMetadata constructFlintMetadata(String indexName, String mapping, String settings) {
    String dataSourceName = options.getDataSourceName();
    FlintMetadataLog metadataLog = metadataLogService.getIndexMetadataLog(indexName, dataSourceName);
    Optional<FlintMetadataLogEntry> latest = metadataLog.getLatest();

    if (latest.isEmpty()) {
      return FlintMetadata.apply(mapping, settings);
    } else {
      return FlintMetadata.apply(mapping, settings, latest.get());
    }
  }
}
