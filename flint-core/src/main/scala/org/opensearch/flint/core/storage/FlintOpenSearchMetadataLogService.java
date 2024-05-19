/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.IOException;
import java.util.logging.Logger;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.flint.core.FlintMetadataLogService;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.metadata.log.DefaultOptimisticTransaction;
import org.opensearch.flint.core.metadata.log.FlintMetadataLog;
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry;
import org.opensearch.flint.core.metadata.log.OptimisticTransaction;
import scala.Some;

public class FlintOpenSearchMetadataLogService implements FlintMetadataLogService {

  private static final Logger LOG = Logger.getLogger(FlintOpenSearchMetadataLogService.class.getName());

  /**
   * Metadata log index name prefix
   */
  public final static String META_LOG_NAME_PREFIX = ".query_execution_request";

  private final FlintOpenSearchStorageClientFactory clientFactory;
  private final FlintOpenSearchIndexOperationService indexOperationService;

  public FlintOpenSearchMetadataLogService(FlintOpenSearchStorageClientFactory clientFactory, FlintOpenSearchIndexOperationService indexOperationService) {
    this.clientFactory = clientFactory;
    this.indexOperationService = indexOperationService;
  }

  // TODO: use getIndexMetadataLog
  @Override
  public <T> OptimisticTransaction<T> startTransaction(
      String indexName, String dataSourceName, boolean forceInit) {
    // TODO: might be a bug for original FlintOpenSearchClient: indexName here needs sanitize too
    LOG.info("Starting transaction on index " + indexName + " and data source " + dataSourceName);
    String metaLogIndexName = constructMetaLogIndexName(dataSourceName);
    try (IRestHighLevelClient client = (IRestHighLevelClient) createClient()) {
      if (client.doesIndexExist(new GetIndexRequest(metaLogIndexName), RequestOptions.DEFAULT)) {
        LOG.info("Found metadata log index " + metaLogIndexName);
      } else {
        if (forceInit) {
          indexOperationService.createIndex(metaLogIndexName, FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_MAPPING(),
              Some.apply(FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_SETTINGS()));
        } else {
          String errorMsg = "Metadata log index not found " + metaLogIndexName;
          LOG.warning(errorMsg);
          throw new IllegalStateException(errorMsg);
        }
      }
      return new DefaultOptimisticTransaction<>(dataSourceName,
          // TODO: this won't compile yet
          new FlintOpenSearchMetadataLog(this, indexName, metaLogIndexName));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index metadata log index exists " + metaLogIndexName, e);
    }
  }

  @Override
  public <T> OptimisticTransaction<T> startTransaction(String indexName, String dataSourceName) {
    return startTransaction(indexName, dataSourceName, false);
  }

  @Override
  public FlintMetadataLog<FlintMetadataLogEntry> getIndexMetadataLog(String indexName, String dataSourceName, boolean forceInit) {
    // TODO: might be a bug for original FlintOpenSearchClient: indexName here needs sanitize too
    // passed in indexName can be sanitized or not (get v.s. getAll)
    LOG.info("Getting metadata log for index " + indexName + " and data source " + dataSourceName);
    String metaLogIndexName = constructMetaLogIndexName(dataSourceName);
    try (IRestHighLevelClient client = (IRestHighLevelClient) createClient()) {
      if (client.doesIndexExist(new GetIndexRequest(metaLogIndexName), RequestOptions.DEFAULT)) {
        LOG.info("Found metadata log index " + metaLogIndexName);
      } else {
        if (forceInit) {
          indexOperationService.createIndex(metaLogIndexName, FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_MAPPING(),
              Some.apply(FlintMetadataLogEntry.QUERY_EXECUTION_REQUEST_SETTINGS()));
        } else {
          String errorMsg = "Metadata log index not found " + metaLogIndexName;
          LOG.warning(errorMsg);
          throw new IllegalStateException(errorMsg);
        }
      }
      return new FlintOpenSearchMetadataLog(this, indexName, metaLogIndexName);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to check if index metadata log index exists " + metaLogIndexName, e);
    }
  }

  @Override
  public FlintMetadataLog<FlintMetadataLogEntry> getIndexMetadataLog(String indexName, String dataSourceName) {
    return getIndexMetadataLog(indexName, dataSourceName, false);
  }

  // TODO: since this depends on indexOperationService anyways, clientFactory is not required at all. Just use indexOperationService.createClient()
  @Override
  public Object createClient() {
    return clientFactory.createClient();
  }

  private String constructMetaLogIndexName(String dataSourceName) {
    return dataSourceName.isEmpty() ? META_LOG_NAME_PREFIX : META_LOG_NAME_PREFIX + "_" + dataSourceName;
  }
}
