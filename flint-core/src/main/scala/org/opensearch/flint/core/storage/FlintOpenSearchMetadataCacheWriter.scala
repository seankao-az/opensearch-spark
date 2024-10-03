/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage

import java.util

import scala.collection.JavaConverters._

import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.PutMappingRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.common.metadata.FlintMetadata
import org.opensearch.flint.core.{FlintOptions, IRestHighLevelClient}
import org.opensearch.flint.core.metadata.FlintIndexMetadataServiceBuilder
import org.opensearch.flint.core.metadata.FlintJsonHelper._
import org.opensearch.flint.core.metadata.ScheduleSecondsParser

import org.apache.spark.internal.Logging

/**
 * TODO: remove this class once extra step for storing some metadata cache in mappings is not
 * needed
 */
class FlintOpenSearchMetadataCacheWriter(options: FlintOptions) extends Logging {
  // TODO: add comment
  private val includeSpec =
    FlintIndexMetadataServiceBuilder
      .build(options)
      .isInstanceOf[FlintOpenSearchIndexMetadataService]

  private val indexMetadataService = new FlintOpenSearchIndexMetadataService(options)

  // Update metadata cache without changing lastRefreshTime and existing FlintMetadata
  def updateMetadataCache(indexName: String, metadata: FlintMetadata): Unit = {
    logInfo(s"Updating metadata cache for $indexName");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new PutMappingRequest(osIndexName)
      val currentLastRefreshTime = metadata.properties.containsKey("lastRefreshTime") match {
        case true => Option(metadata.properties.get("lastRefreshTime").asInstanceOf[Long])
        case false => None
      }
      request.source(serialize(metadata, currentLastRefreshTime), XContentType.JSON)
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

  // Update lastRefreshTime without changing metadata cache
  def updateLastRefreshTime(indexName: String, lastRefreshTime: Long): Unit = {
    logInfo(s"Updating last refresh time for $indexName");
    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(indexName)
    var client: IRestHighLevelClient = null
    try {
      client = OpenSearchClientUtils.createClient(options)
      val request = new PutMappingRequest(osIndexName)
      val metadata = indexMetadataService.getIndexMetadata(indexName)
      request.source(serialize(metadata, Option(lastRefreshTime)), XContentType.JSON)
      client.updateIndexMapping(request, RequestOptions.DEFAULT)
    } catch {
      case e: Exception =>
        throw new IllegalStateException(
          s"Failed to update last refresh time for Flint index $osIndexName",
          e)
    } finally
      if (client != null) {
        client.close()
      }
  }

  private def serialize(metadata: FlintMetadata, lastRefreshTime: Option[Long]): String = {
    try {
      buildJson(builder => {
        objectField(builder, "_meta") {
          if (includeSpec) {
            // Copied from FlintOpenSearchIndexMetadataService
            builder
              .field("version", metadata.version.version)
              .field("name", metadata.name)
              .field("kind", metadata.kind)
              .field("source", metadata.source)
              .field("indexedColumns", metadata.indexedColumns)

            if (metadata.latestId.isDefined) {
              builder.field("latestId", metadata.latestId.get)
            }
            optionalObjectField(builder, "options", metadata.options)
            // End of copy
          }

          optionalObjectField(
            builder,
            "properties",
            buildPropertiesMap(metadata, lastRefreshTime))
        }
        builder.field("properties", metadata.schema)
      })
    } catch {
      case e: Exception =>
        throw new IllegalStateException("Failed to jsonify cache metadata", e)
    }
  }

  private def buildPropertiesMap(
      metadata: FlintMetadata,
      lastRefreshTime: Option[Long]): util.Map[String, AnyRef] = {
    // Fixed dummy data
    val mockSourceTables: Array[String] = Array(
      "dataSourceName.default.logGroups(logGroupIdentifier:['arn:aws:logs:us-east-1:123456:test-llt-xa', 'arn:aws:logs:us-east-1:123456:sample-lg-1'])")

    val metadataCacheProperties = {
      val baseMap = Map("metadataCacheVersion" -> "1.0", "sourceTables" -> mockSourceTables)
      val refreshIntervalMap = {
        // TODO: check condition for external scheduler index
        if (metadata.options.getOrDefault("auto_refresh", "false") == "true" && metadata.options
            .containsKey("refresh_interval")) {
          val refreshInterval =
            ScheduleSecondsParser.parse(metadata.options.get("refresh_interval").toString)
          Map("refreshInterval" -> refreshInterval)
        } else {
          Map.empty
        }
      }
      val lastRefreshTimeMap = lastRefreshTime match {
        case Some(lastRefreshTime) => Map("lastRefreshTime" -> lastRefreshTime)
        case None => Map.empty
      }
      (baseMap ++ refreshIntervalMap ++ lastRefreshTimeMap)
        .mapValues(_.asInstanceOf[AnyRef])
    }

    if (includeSpec) {
      (metadataCacheProperties ++ metadata.properties.asScala).asJava
    } else {
      metadataCacheProperties.asJava
    }
  }
}
