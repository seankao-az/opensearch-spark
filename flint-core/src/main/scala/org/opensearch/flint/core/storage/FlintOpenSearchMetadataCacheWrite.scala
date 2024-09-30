/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.{GetIndexRequest, PutMappingRequest}
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.core.{FlintOptions, IRestHighLevelClient}
import org.opensearch.flint.core.metadata.FlintJsonHelper._
import org.opensearch.flint.core.metadata.ScheduleSecondsParser

import org.apache.spark.internal.Logging

/**
 * TODO: remove this class once extra step for storing some metadata in mappings is not needed
 */
class FlintOpenSearchMetadataCacheWrite(options: FlintOptions) extends Logging {

  // Update metadata cache without changing lastRefreshTime
  def updateMetadataCache(indexName: String, metadata: FlintMetadata): Unit = {
    logInfo(s"Updating cache metadata for $indexName");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new PutMappingRequest(osIndexName)
      val currentLastRefreshTime = getMetadataCache(indexName).lastRefreshTime
      val metadataCache =
        FlintMetadataCache(metadata).copy(lastRefreshTime = currentLastRefreshTime)
      request.source(serialize(metadataCache), XContentType.JSON)
      client.updateIndexMapping(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to update Flint index $osIndexName", e)
    } finally
      if (client != null) {
        client.close()
      }
  }

  // Update lastRefreshTime without changing metadata cache
  def updateLastRefreshTime(indexName: String, lastRefreshTime: Long): Unit = {
    logInfo(s"Updating last refresh time for $indexName");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new PutMappingRequest(osIndexName)
      val metadataCache =
        getMetadataCache(indexName).copy(lastRefreshTime = Option(lastRefreshTime))
      request.source(serialize(metadataCache), XContentType.JSON)
      client.updateIndexMapping(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(s"Failed to update Flint index $osIndexName", e)
    } finally
      if (client != null) {
        client.close()
      }
  }

  private def getMetadataCache(indexName: String): FlintMetadataCache = {
    logInfo(s"Fetching cache metadata for $indexName")
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new GetIndexRequest(osIndexName)
      val response = client.getIndex(request, RequestOptions.DEFAULT)
      val mapping = response.getMappings.get(osIndexName)
      deserialize(mapping.source.string)
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to get cache metadata for " + osIndexName, e)
    } finally
      if (client != null) {
        client.close()
      }
  }

  private def serialize(metadataCache: FlintMetadataCache): String = {
    try {
      buildJson(builder => {
        objectField(builder, "_meta") {
          objectField(builder, "properties") {
            builder.field("sourceTables", metadataCache.sourceTables)
            if (metadataCache.refreshInterval.isDefined) {
              builder.field("refreshInterval", metadataCache.refreshInterval.get)
            }
            if (metadataCache.lastRefreshTime.isDefined) {
              builder.field("lastRefreshTime", metadataCache.lastRefreshTime.get)
            }
          }
        }
        builder.field("properties", metadataCache.schema)
      })
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to jsonify cache metadata", e)
    }
  }

  private def deserialize(content: String): FlintMetadataCache = {
    try {
      var refreshInterval: Option[Int] = None
      var sourceTables: Array[String] = Array()
      var lastRefreshTime: Option[Long] = None
      var schema: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()
      parseJson(content) { (parser, fieldName) =>
        {
          fieldName match {
            case "_meta" =>
              parseObjectField(parser) { (parser, innerFieldName) =>
                {
                  innerFieldName match {
                    case "refreshInterval" => refreshInterval = Option(parser.intValue())
                    case "sourceTables" =>
                      parseArrayField(parser) {
                        sourceTables = sourceTables :+ parser.text()
                      }
                    case "lastRefreshTime" => lastRefreshTime = Option(parser.longValue())
                    case _ => // Ignore
                  }
                }
              }
            case "properties" =>
              schema = parser.map()
            case _ => // Ignore
          }
        }
      }
      FlintMetadataCache(refreshInterval, sourceTables, lastRefreshTime, schema)
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to parse cache metadata JSON", e)
    }
  }
}

case class FlintMetadataCache(
    refreshInterval: Option[Int],
    sourceTables: Array[String],
    lastRefreshTime: Option[Long],
    schema: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef])

object FlintMetadataCache {
  def apply(metadata: FlintMetadata): FlintMetadataCache = {
    val refreshInterval: Option[Int] = {
      if (metadata.options.getOrDefault("auto_refresh", "false") == "true" && metadata.options
          .containsKey("refresh_interval")) {
        Option(ScheduleSecondsParser.parse(metadata.options.get("refresh_interval").toString))
      } else {
        None
      }
    }
    FlintMetadataCache(
      refreshInterval,
      Array("mockCw.default.logGroup1", "mockCw.default.logGroup2"),
      None,
      metadata.schema)
  }
}
