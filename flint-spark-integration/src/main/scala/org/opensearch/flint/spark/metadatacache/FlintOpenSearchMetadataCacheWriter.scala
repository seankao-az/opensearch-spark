/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.metadatacache

import scala.collection.JavaConverters._

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.PutMappingRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.core.{FlintOptions, IRestHighLevelClient}
import org.opensearch.flint.core.metadata.FlintJsonHelper._
import org.opensearch.flint.core.storage.{OpenSearchClientUtils, OpenSearchIndexRetriever}

import org.apache.spark.internal.Logging

/**
 * Writes {@link FlintMetadataCache} to index mappings `_meta` field for frontend user to access.
 */
class FlintOpenSearchMetadataCacheWriter(options: FlintOptions)
    extends FlintMetadataCacheWriter
    with OpenSearchIndexRetriever
    with Logging {

  override def updateMetadataCache(indexName: String, metadata: FlintMetadata): Unit = {
    logInfo(s"Updating metadata cache for $indexName with $metadata");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val existingMapping = getIndexInfo(osIndexName, options).mapping.sourceAsMap().asScala.toMap
      val metadataCacheProperties = FlintMetadataCache(metadata).toMap
      val mergedMapping = mergeMapping(existingMapping, metadataCacheProperties)
      val serialized = buildJson(builder => {
        builder.field("_meta", mergedMapping.get("_meta").get)
        builder.field("properties", mergedMapping.get("properties").get)
      })
      val request = new PutMappingRequest(osIndexName)
      request.source(serialized, XContentType.JSON)
      client.updateIndexMapping(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to update metadata cache for Flint index $osIndexName",
          e)
    } finally
      if (client != null) {
        client.close()
      }
  }

  private def mergeMapping(existingMapping: Map[String, AnyRef], metadataCacheProperties: Map[String, AnyRef]): Map[String, AnyRef] = {
    val meta = existingMapping.getOrElse("_meta", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    val properties = meta.getOrElse("properties", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    val updatedProperties = properties ++ metadataCacheProperties
    val updatedMeta = meta + ("properties" -> updatedProperties.asJava)
    existingMapping + ("_meta" -> updatedMeta.asJava)
  }
}
