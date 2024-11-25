/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.cluster.metadata.MappingMetadata
import org.opensearch.common.settings.Settings
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.IRestHighLevelClient

trait OpenSearchIndexRetriever {
  case class IndexInfo(mapping: MappingMetadata, settings: Settings)

  /**
   * Get index info from OpenSearch cluster
   *
   * @param osIndexName
   *   OpenSearch index name
   * @param options
   *   Flint options for creating OpenSearch client
   * @return
   *   index mapping and settings
   */
  def getIndexInfo(osIndexName: String, options: FlintOptions): IndexInfo = {
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new GetIndexRequest(osIndexName)
      val response = client.getIndex(request, RequestOptions.DEFAULT)
      val mapping = response.getMappings.get(osIndexName)
      val settings = response.getSettings.get(osIndexName)
      IndexInfo(mapping, settings)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to get index info for $osIndexName", e)
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }
}
