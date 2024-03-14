/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.Serialization
import org.opensearch.flint.core.{FlintClient, FlintClientBuilder}
import org.opensearch.flint.core.metadata.log.FlintMetadataLogEntry.IndexState._
import org.opensearch.flint.core.metadata.log.OptimisticTransaction.NO_LOG_ENTRY
import org.opensearch.flint.spark.FlintSparkIndex.ID_COLUMN
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh
import org.opensearch.flint.spark.refresh.FlintSparkIndexRefresh.RefreshMode.AUTO
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy.SkippingKindSerializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.{DOC_ID_COLUMN_NAME, IGNORE_DOC_ID_COLUMN}

/**
 * Flint Spark integration API entrypoint.
 */
class FlintSpark(val spark: SparkSession) extends Logging {

  /** Flint spark configuration */
  private val flintSparkConf: FlintSparkConf =
    FlintSparkConf(
      Map(
        DOC_ID_COLUMN_NAME.optionKey -> ID_COLUMN,
        IGNORE_DOC_ID_COLUMN.optionKey -> "true").asJava)

  /** Flint client for low-level index operation */
  private val flintClient: FlintClient = FlintClientBuilder.build(flintSparkConf.flintOptions())

  /** Required by json4s parse function */
  implicit val formats: Formats = Serialization.formats(NoTypeHints) + SkippingKindSerializer

  /**
   * Data source name. Assign empty string in case of backward compatibility. TODO: remove this in
   * future
   */
  private val dataSourceName: String =
    spark.conf.getOption("spark.flint.datasource.name").getOrElse("")

  /** Flint Spark index monitor */
  private val flintIndexMonitor: FlintSparkIndexMonitor =
    new FlintSparkIndexMonitor(spark, flintClient, dataSourceName)

  /**
   * Create index builder for creating index with fluent API.
   *
   * @return
   *   index builder
   */
  def skippingIndex(): FlintSparkSkippingIndex.Builder = {
    new FlintSparkSkippingIndex.Builder(this)
  }

  /**
   * Create index builder for creating index with fluent API.
   *
   * @return
   *   index builder
   */
  def coveringIndex(): FlintSparkCoveringIndex.Builder = {
    new FlintSparkCoveringIndex.Builder(this)
  }

  /**
   * Create materialized view builder for creating mv with fluent API.
   *
   * @return
   *   mv builder
   */
  def materializedView(): FlintSparkMaterializedView.Builder = {
    new FlintSparkMaterializedView.Builder(this)
  }

  /**
   * Create the given index with metadata.
   *
   * @param index
   *   Flint index to create
   * @param ignoreIfExists
   *   Ignore existing index
   */
  def createIndex(index: FlintSparkIndex, ignoreIfExists: Boolean = false): Unit = {
    logInfo(s"Creating Flint index $index with ignoreIfExists $ignoreIfExists")
    val indexName = index.name()
    if (flintClient.exists(indexName)) {
      if (!ignoreIfExists) {
        throw new IllegalStateException(s"Flint index $indexName already exists")
      }
    } else {
      val metadata = index.metadata()
      try {
        flintClient
          .startTransaction(indexName, dataSourceName, true)
          .initialLog(latest => latest.state == EMPTY || latest.state == DELETED)
          .transientLog(latest => latest.copy(state = CREATING))
          .finalLog(latest => latest.copy(state = ACTIVE))
          .commit(latest =>
            if (latest == null) { // in case transaction capability is disabled
              flintClient.createIndex(indexName, metadata)
            } else {
              logInfo(s"Creating index with metadata log entry ID ${latest.id}")
              flintClient.createIndex(indexName, metadata.copy(latestId = Some(latest.id)))
            })
        logInfo("Create index complete")
      } catch {
        case e: Exception =>
          logError("Failed to create Flint index", e)
          throw new IllegalStateException("Failed to create Flint index")
      }
    }
  }

  /**
   * Start refreshing index data according to the given mode.
   *
   * @param indexName
   *   index name
   * @return
   *   refreshing job ID (empty if batch job for now)
   */
  def refreshIndex(indexName: String): Option[String] = {
    logInfo(s"Refreshing Flint index $indexName")
    val index = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
    val indexRefresh = FlintSparkIndexRefresh.create(indexName, index)

    try {
      flintClient
        .startTransaction(indexName, dataSourceName)
        .initialLog(latest => latest.state == ACTIVE)
        .transientLog(latest =>
          latest.copy(state = REFRESHING, createTime = System.currentTimeMillis()))
        .finalLog(latest => {
          // Change state to active if full, otherwise update index state regularly
          if (indexRefresh.refreshMode == AUTO) {
            logInfo("Scheduling index state monitor")
            flintIndexMonitor.startMonitor(indexName)
            latest
          } else {
            logInfo("Updating index state to active")
            latest.copy(state = ACTIVE)
          }
        })
        .commit(_ => indexRefresh.start(spark, flintSparkConf))
    } catch {
      case e: Exception =>
        logError("Failed to refresh Flint index", e)
        throw new IllegalStateException("Failed to refresh Flint index")
    }
  }

  /**
   * Describe all Flint indexes whose name matches the given pattern.
   *
   * @param indexNamePattern
   *   index name pattern which may contains wildcard
   * @return
   *   Flint index list
   */
  def describeIndexes(indexNamePattern: String): Seq[FlintSparkIndex] = {
    logInfo(s"Describing indexes with pattern $indexNamePattern")
    if (flintClient.exists(indexNamePattern)) {
      flintClient
        .getAllIndexMetadata(indexNamePattern)
        .asScala
        .map(FlintSparkIndexFactory.create)
    } else {
      Seq.empty
    }
  }

  /**
   * Describe a Flint index.
   *
   * @param indexName
   *   index name
   * @return
   *   Flint index
   */
  def describeIndex(indexName: String): Option[FlintSparkIndex] = {
    logInfo(s"Describing index name $indexName")
    if (flintClient.exists(indexName)) {
      val metadata = flintClient.getIndexMetadata(indexName)
      val index = FlintSparkIndexFactory.create(metadata)
      Some(index)
    } else {
      Option.empty
    }
  }

  /**
   * Update index with index options.
   * State remains unchanged throughout options update, since refresh job is not affected.
   *
   * @param indexName
   *   index name
   * @param updateOptions
   *   options to update
   */
  def updateIndexOptions(indexName: String, updateOptions: Map[String, String]): Unit = {
    logInfo(s"Updating options for Flint index $indexName")
    val index = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
    try {
      flintClient
        .startTransaction(indexName, dataSourceName)
        .initialLog(latest => latest.state == ACTIVE || latest.state == REFRESHING)
        .transientLog(latest => latest)
        .finalLog(latest => latest)
        .commit(latest => {
          // TODO: validation
          val updateIndex = buildUpdateIndex(index, updateOptions)
          val metadata = updateIndex.metadata()
          if (latest == null) { // in case transaction capability is disabled
            flintClient.updateIndex(indexName, metadata)
          } else {
            logInfo(s"Updating index with metadata log entry ID ${latest.id}")
            flintClient.updateIndex(indexName, metadata.copy(latestId = Some(latest.id)))
          }
        })
      logInfo("Update index complete")
    } catch {
      case e: Exception =>
        logError("Failed to update Flint index options", e)
        // TODO: more informative
        throw new IllegalStateException("Failed to update Flint index options")
    }
  }

  /**
   * Update job for index.
   *
   * @param indexName
   * index name
   */
  def updateIndexJob(indexName: String): Unit = {
    logInfo(s"Updating job for Flint index $indexName")
    val index = describeIndex(indexName)
      .getOrElse(throw new IllegalStateException(s"Index $indexName doesn't exist"))
    try {
      // Assumes that auto refresh index must be updated to non auto refresh index, and vice versa.
      // Options validation should ensure this.
      if (index.options.autoRefresh()) {
        refreshIndex(indexName)
      } else {
        cancelIndex(indexName)
      }
    } catch {
      case e: Exception =>
        logError("Failed to update Flint index job", e)
        // TODO: more informative
        throw new IllegalStateException("Failed to update Flint index job")
    }
  }

  /**
   * Cancel refreshing job associated to index
   *
   * @param indexName
   * index name
   * @return
   * true if exist and cancelled, otherwise false
   */
  def cancelIndex(indexName: String): Boolean = {
    logInfo(s"Cancelling Flint index $indexName")
    if (flintClient.exists(indexName)) {
      try {
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(latest => latest.state == REFRESHING)
          .transientLog(latest => latest.copy(state = CANCELLING))
          .finalLog(latest => latest.copy(state = ACTIVE))
          .commit(_ => {
            flintIndexMonitor.stopMonitor(indexName)
            stopRefreshingJob(indexName)
            true
          })
      } catch {
        case e: Exception =>
          logError("Failed to cancel Flint index", e)
          throw new IllegalStateException("Failed to cancel Flint index")
      }
    } else {
      logInfo("Flint index to be cancelled doesn't exist")
      false
    }
  }

  /**
   * Delete index and refreshing job associated.
   *
   * @param indexName
   *   index name
   * @return
   *   true if exist and deleted, otherwise false
   */
  def deleteIndex(indexName: String): Boolean = {
    logInfo(s"Deleting Flint index $indexName")
    if (flintClient.exists(indexName)) {
      try {
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(latest => latest.state == ACTIVE || latest.state == REFRESHING)
          .transientLog(latest => latest.copy(state = DELETING))
          .finalLog(latest => latest.copy(state = DELETED))
          .commit(_ => {
            // TODO: share same transaction for now
            flintIndexMonitor.stopMonitor(indexName)
            stopRefreshingJob(indexName)
            true
          })
      } catch {
        case e: Exception =>
          logError("Failed to delete Flint index", e)
          throw new IllegalStateException("Failed to delete Flint index")
      }
    } else {
      logInfo("Flint index to be deleted doesn't exist")
      false
    }
  }

  /**
   * Delete a Flint index physically.
   *
   * @param indexName
   *   index name
   * @return
   *   true if exist and deleted, otherwise false
   */
  def vacuumIndex(indexName: String): Boolean = {
    logInfo(s"Vacuuming Flint index $indexName")
    if (flintClient.exists(indexName)) {
      try {
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(latest => latest.state == DELETED)
          .transientLog(latest => latest.copy(state = VACUUMING))
          .finalLog(_ => NO_LOG_ENTRY)
          .commit(_ => {
            flintClient.deleteIndex(indexName)
            true
          })
      } catch {
        case e: Exception =>
          logError("Failed to vacuum Flint index", e)
          throw new IllegalStateException("Failed to vacuum Flint index")
      }
    } else {
      logInfo("Flint index to vacuum doesn't exist")
      false
    }
  }

  /**
   * Recover index job.
   *
   * @param indexName
   *   index name
   */
  def recoverIndex(indexName: String): Boolean = {
    logInfo(s"Recovering Flint index $indexName")
    val index = describeIndex(indexName)
    if (index.exists(_.options.autoRefresh())) {
      try {
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(latest => Set(ACTIVE, REFRESHING, FAILED).contains(latest.state))
          .transientLog(latest =>
            latest.copy(state = RECOVERING, createTime = System.currentTimeMillis()))
          .finalLog(latest => {
            flintIndexMonitor.startMonitor(indexName)
            latest.copy(state = REFRESHING)
          })
          .commit(_ =>
            FlintSparkIndexRefresh
              .create(indexName, index.get)
              .start(spark, flintSparkConf))

        logInfo("Recovery complete")
        true
      } catch {
        case e: Exception =>
          logError("Failed to recover Flint index", e)
          throw new IllegalStateException("Failed to recover Flint index")
      }
    } else {
      logInfo("Index to be recovered either doesn't exist or not auto refreshed")
      if (index.isEmpty) {
        /*
         * If execution reaches this point, it indicates that the Flint index is corrupted.
         * In such cases, clean up the metadata log, as the index data no longer exists.
         * There is a very small possibility that users may recreate the index in the
         * interim, but metadata log get deleted by this cleanup process.
         */
        logWarning("Cleaning up metadata log as index data has been deleted")
        flintClient
          .startTransaction(indexName, dataSourceName)
          .initialLog(_ => true)
          .finalLog(_ => NO_LOG_ENTRY)
          .commit(_ => {})
      }
      false
    }
  }

  /**
   * Build data frame for querying the given index. This is mostly for unit test convenience.
   *
   * @param indexName
   *   index name
   * @return
   *   index query data frame
   */
  def queryIndex(indexName: String): DataFrame = {
    spark.read.format(FLINT_DATASOURCE).load(indexName)
  }

  private def buildUpdateIndex(index: FlintSparkIndex, updateOptions: Map[String, String]): FlintSparkIndex = {
    val options = index.options.options ++ updateOptions
    val metadata = index.metadata().copy(options = options.mapValues(_.asInstanceOf[AnyRef]).asJava)
    FlintSparkIndexFactory.create(metadata)
  }

  private def stopRefreshingJob(indexName: String): Unit = {
    logInfo(s"Terminating refreshing job $indexName")
    val job = spark.streams.active.find(_.name == indexName)
    if (job.isDefined) {
      job.get.stop()
    } else {
      logWarning("Refreshing job not found")
    }
  }
}
