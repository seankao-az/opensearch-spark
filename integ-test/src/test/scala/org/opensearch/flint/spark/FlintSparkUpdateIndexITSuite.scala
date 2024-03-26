/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s.native.JsonMethods._
import org.opensearch.client.RequestOptions
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.opensearch.index.query.QueryBuilders
import org.opensearch.index.reindex.DeleteByQueryRequest
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FlintSparkUpdateIndexITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.test"
  private val testIndex = getSkippingIndexName(testTable)

  override def beforeEach(): Unit = {
    super.beforeEach()
    createPartitionedMultiRowTable(testTable)
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    deleteTestIndex(testIndex)
    sql(s"DROP TABLE $testTable")
  }

  test("update index with index options successfully") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addValueSet("address")
        .options(FlintSparkIndexOptions(Map(
          "auto_refresh" -> "false",
          "incremental_refresh" -> "true",
          "refresh_interval" -> "1 Minute",
          "checkpoint_location" -> checkpointDir.getAbsolutePath,
          "index_settings" -> "{\"number_of_shards\": 3,\"number_of_replicas\": 2}")))
        .create()
      val indexInitial = flint.describeIndex(testIndex).get

      // Update index options
      val updateOptions =
        FlintSparkIndexOptions(Map("auto_refresh" -> "true", "incremental_refresh" -> "false"))
      val updatedIndex = flint.skippingIndex().copyWithUpdate(indexInitial, updateOptions)
      flint.updateIndex(updatedIndex)

      // Verify index after update
      val indexFinal = flint.describeIndex(testIndex).get
      val optionJson =
        compact(render(parse(indexFinal.metadata().getContent) \ "_meta" \ "options"))
      optionJson should matchJson(s"""
          | {
          |   "auto_refresh": "true",
          |   "incremental_refresh": "false",
          |   "refresh_interval": "1 Minute",
          |   "checkpoint_location": "${checkpointDir.getAbsolutePath}",
          |   "index_settings": "{\\\"number_of_shards\\\": 3,\\\"number_of_replicas\\\": 2}"
          | }
          |""".stripMargin)

      // Load index options from index mapping (verify OS index setting in SQL IT)
      indexFinal.options.autoRefresh() shouldBe true
      indexFinal.options.incrementalRefresh() shouldBe false
      indexFinal.options.refreshInterval() shouldBe Some("1 Minute")
      indexFinal.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
      indexFinal.options.indexSettings() shouldBe
        Some("{\"number_of_shards\": 3,\"number_of_replicas\": 2}")
    }
  }

  test("should fail if update index without changing auto_refresh option") {
    // Create auto refresh Flint index
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testIndex)
    var index = flint.describeIndex(testIndex).get

    // auto_refresh remains true
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(index, FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      flint.updateIndex(updatedIndex)
    }
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          index,
          FlintSparkIndexOptions(
            Map("auto_refresh" -> "true", "checkpoint_location" -> "s3a://test/")))
      flint.updateIndex(updatedIndex)
    }

    // auto_refresh not provided
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(index, FlintSparkIndexOptions(Map("incremental_refresh" -> "true")))
      flint.updateIndex(updatedIndex)
    }
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          index,
          FlintSparkIndexOptions(Map("checkpoint_location" -> "s3a://test/")))
      flint.updateIndex(updatedIndex)
    }

    deleteTestIndex(testIndex)

    // Create full refresh Flint index
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    index = flint.describeIndex(testIndex).get

    // auto_refresh remains false
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(index, FlintSparkIndexOptions(Map("auto_refresh" -> "false")))
      flint.updateIndex(updatedIndex)
    }
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          index,
          FlintSparkIndexOptions(
            Map("auto_refresh" -> "false", "checkpoint_location" -> "s3a://test/")))
      flint.updateIndex(updatedIndex)
    }

    // auto_refresh not provided
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(index, FlintSparkIndexOptions(Map("incremental_refresh" -> "true")))
      flint.updateIndex(updatedIndex)
    }
    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          index,
          FlintSparkIndexOptions(Map("checkpoint_location" -> "s3a://test/")))
      flint.updateIndex(updatedIndex)
    }
  }

  test("should succeed if convert to full refresh with allowed options") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testIndex)

    // Update index options
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(
        indexInitial,
        FlintSparkIndexOptions(Map("auto_refresh" -> "false", "incremental_refresh" -> "false")))
    flint.updateIndex(updatedIndex)

    // Verify index after update
    val indexFinal = flint.describeIndex(testIndex).get
    indexFinal.options.autoRefresh() shouldBe false
    indexFinal.options.incrementalRefresh() shouldBe false
  }

  test("should fail if convert to full refresh with disallowed options") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
        .options(FlintSparkIndexOptions(Map(
          "auto_refresh" -> "true",
          "refresh_interval" -> "1 Minute",
          "checkpoint_location" -> checkpointDir.getAbsolutePath)))
        .create()
      flint.refreshIndex(testIndex)
      val indexInitial = flint.describeIndex(testIndex).get

      the[IllegalArgumentException] thrownBy {
        val updatedIndex = flint
          .skippingIndex()
          .copyWithUpdate(
            indexInitial,
            FlintSparkIndexOptions(
              Map("auto_refresh" -> "false", "checkpoint_location" -> "s3a://test/")))
        flint.updateIndex(updatedIndex)
      }
      the[IllegalArgumentException] thrownBy {
        val updatedIndex = flint
          .skippingIndex()
          .copyWithUpdate(
            indexInitial,
            FlintSparkIndexOptions(
              Map("auto_refresh" -> "false", "refresh_interval" -> "5 Minute")))
        flint.updateIndex(updatedIndex)
      }
      the[IllegalArgumentException] thrownBy {
        val updatedIndex = flint
          .skippingIndex()
          .copyWithUpdate(
            indexInitial,
            FlintSparkIndexOptions(Map("auto_refresh" -> "false", "watermark_delay" -> "1 Minute")))
        flint.updateIndex(updatedIndex)
      }
    }
  }

  test("should succeed if convert to incremental refresh with refresh_interval") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testIndex)

    // Update index options
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(
        indexInitial,
        FlintSparkIndexOptions(
          Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "refresh_interval" -> "1 Minute")))
    flint.updateIndex(updatedIndex)

    // Verify index after update
    val indexFinal = flint.describeIndex(testIndex).get
    indexFinal.options.autoRefresh() shouldBe false
    indexFinal.options.incrementalRefresh() shouldBe true
    indexFinal.options.refreshInterval() shouldBe Some("1 Minute")
  }

  test("should succeed if convert to incremental refresh with checkpoint_location") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testIndex)

    // Update index options
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(
        indexInitial,
        FlintSparkIndexOptions(
          Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "checkpoint_location" -> "s3a://test/")))
    flint.updateIndex(updatedIndex)

    // Verify index after update
    val indexFinal = flint.describeIndex(testIndex).get
    indexFinal.options.autoRefresh() shouldBe false
    indexFinal.options.incrementalRefresh() shouldBe true
    indexFinal.options.checkpointLocation() shouldBe Some("s3a://test/")
  }

  test("should succeed if convert to incremental refresh with watermark_delay") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testIndex)

    // Update index options
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(
        indexInitial,
        FlintSparkIndexOptions(
          Map(
            "auto_refresh" -> "false",
            "incremental_refresh" -> "true",
            "watermark_delay" -> "1 Minute")))
    flint.updateIndex(updatedIndex)

    // Verify index after update
    val indexFinal = flint.describeIndex(testIndex).get
    indexFinal.options.autoRefresh() shouldBe false
    indexFinal.options.incrementalRefresh() shouldBe true
    indexFinal.options.watermarkDelay() shouldBe Some("1 Minute")
  }

  test("should fail if convert to incremental refresh with disallowed options") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testIndex)
    val indexInitial = flint.describeIndex(testIndex).get

    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "false",
              "incremental_refresh" -> "true",
              "output_mode" -> "complete")))
      flint.updateIndex(updatedIndex)
    }
  }

  test("should succeed if convert to auto refresh with refresh_interval") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    // Update index options
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(
        indexInitial,
        FlintSparkIndexOptions(Map("auto_refresh" -> "true", "refresh_interval" -> "5 Minute")))
    flint.updateIndex(updatedIndex)

    // Verify index after update
    val indexFinal = flint.describeIndex(testIndex).get
    indexFinal.options.autoRefresh() shouldBe true
    indexFinal.options.incrementalRefresh() shouldBe false
    indexFinal.options.refreshInterval() shouldBe Some("5 Minute")
  }

  test("should succeed if convert to auto refresh with checkpoint_location") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .create()

      // Update index options
      val indexInitial = flint.describeIndex(testIndex).get
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)))
      flint.updateIndex(updatedIndex)

      // Verify index after update
      val indexFinal = flint.describeIndex(testIndex).get
      indexFinal.options.autoRefresh() shouldBe true
      indexFinal.options.incrementalRefresh() shouldBe false
      indexFinal.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
    }
  }

  test("should succeed if convert to auto refresh with watermark_delay") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    // Update index options
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex = flint
      .skippingIndex()
      .copyWithUpdate(
        indexInitial,
        FlintSparkIndexOptions(Map("auto_refresh" -> "true", "watermark_delay" -> "5 Minute")))
    flint.updateIndex(updatedIndex)

    // Verify index after update
    val indexFinal = flint.describeIndex(testIndex).get
    indexFinal.options.autoRefresh() shouldBe true
    indexFinal.options.incrementalRefresh() shouldBe false
    indexFinal.options.watermarkDelay() shouldBe Some("5 Minute")
  }

  test("should fail if convert to auto refresh with disallowed options") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    val indexInitial = flint.describeIndex(testIndex).get

    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(Map("auto_refresh" -> "true", "output_mode" -> "complete")))
      flint.updateIndex(updatedIndex)
    }
  }

  test("should fail if convert to invalid refresh mode") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    var indexInitial = flint.describeIndex(testIndex).get

    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(Map("auto_refresh" -> "true", "incremental_refresh" -> "true")))
      flint.updateIndex(updatedIndex)
    }

    deleteTestIndex(testIndex)

    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()
    flint.refreshIndex(testIndex)
    indexInitial = flint.describeIndex(testIndex).get

    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("incremental_refresh" -> "true")))
      flint.updateIndex(updatedIndex)
    }

    deleteTestIndex(testIndex)

    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("incremental_refresh" -> "true")))
      .create()
    indexInitial = flint.describeIndex(testIndex).get

    the[IllegalArgumentException] thrownBy {
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      flint.updateIndex(updatedIndex)
    }
  }

  test("update index should fail if index is updated by others before transaction starts") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .create()

      // This update will be delayed
      val indexInitial = flint.describeIndex(testIndex).get
      val updatedIndexObsolete = flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "true", "checkpoint_location" -> checkpointDir.getAbsolutePath)))

      // This other update finishes first, converting to auto refresh
      flint.updateIndex(flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "true"))))
      // Adding another update to convert to full refresh, so the obsolete update doesn't fail for option validation or state validation
      val indexUpdated = flint.describeIndex(testIndex).get
      flint.updateIndex(flint
        .skippingIndex()
        .copyWithUpdate(indexUpdated, FlintSparkIndexOptions(Map("auto_refresh" -> "false"))))

      // This update trying to update an obsolete index should fail
      the[IllegalStateException] thrownBy
        flint.updateIndex(updatedIndexObsolete)

      // Verify index after update
      val indexFinal = flint.describeIndex(testIndex).get
      indexFinal.options.autoRefresh() shouldBe false
      indexFinal.options.checkpointLocation() shouldBe empty
    }
  }

  test("update full refresh index to auto refresh should start job") {
    // Create full refresh Flint index
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()
    spark.streams.active.find(_.name == testIndex) shouldBe empty
    flint.queryIndex(testIndex).collect().toSet should have size 0

    // Update Flint index to auto refresh and wait for complete
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex =
      flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
    val jobId = flint.updateIndex(updatedIndex)
    jobId shouldBe defined

    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    flint.queryIndex(testIndex).collect().toSet should have size 2
  }

  test("update incremental refresh index to auto refresh should start job") {
    withTempDir { checkpointDir =>
      // Create incremental refresh Flint index and wait for complete
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(
          FlintSparkIndexOptions(
            Map(
              "incremental_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)))
        .create()

      flint.refreshIndex(testIndex) shouldBe empty
      spark.streams.active.find(_.name == testIndex) shouldBe empty
      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Delete all index data intentionally and generate a new source file
      openSearchClient.deleteByQuery(
        new DeleteByQueryRequest(testIndex).setQuery(QueryBuilders.matchAllQuery()),
        RequestOptions.DEFAULT)
      sql(s"""
             | INSERT INTO $testTable
             | PARTITION (year=2023, month=4)
             | VALUES ('Hello', 35, 'Vancouver')
             | """.stripMargin)

      // Update Flint index to auto refresh and wait for complete
      val indexInitial = flint.describeIndex(testIndex).get
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(Map("auto_refresh" -> "true", "incremental_refresh" -> "false")))
      val jobId = flint.updateIndex(updatedIndex)
      jobId shouldBe defined

      val job = spark.streams.get(jobId.get)
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      // Expect to only refresh the new file
      flint.queryIndex(testIndex).collect().toSet should have size 1
    }
  }

  test("update auto refresh index to full refresh should stop job") {
    // Create auto refresh Flint index and wait for complete
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .options(FlintSparkIndexOptions(Map("auto_refresh" -> "true")))
      .create()

    val jobId = flint.refreshIndex(testIndex)
    val job = spark.streams.get(jobId.get)
    failAfter(streamingTimeout) {
      job.processAllAvailable()
    }

    flint.queryIndex(testIndex).collect().toSet should have size 2

    // Update Flint index to full refresh
    val indexInitial = flint.describeIndex(testIndex).get
    val updatedIndex =
      flint
        .skippingIndex()
        .copyWithUpdate(indexInitial, FlintSparkIndexOptions(Map("auto_refresh" -> "false")))
    flint.updateIndex(updatedIndex) shouldBe empty

    // Expect refresh job to be stopped
    spark.streams.active.find(_.name == testIndex) shouldBe empty

    // Generate a new source file
    sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=4)
           | VALUES ('Hello', 35, 'Vancouver')
           | """.stripMargin)

    // Index shouldn't be refreshed
    flint.queryIndex(testIndex).collect().toSet should have size 2

    // Full refresh after update
    flint.refreshIndex(testIndex) shouldBe empty
    flint.queryIndex(testIndex).collect().toSet should have size 3
  }

  test("update auto refresh index to incremental refresh should stop job") {
    withTempDir { checkpointDir =>
      // Create auto refresh Flint index and wait for complete
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year", "month")
        .options(FlintSparkIndexOptions(
          Map("auto_refresh" -> "true", "checkpoint_location" -> checkpointDir.getAbsolutePath)))
        .create()

      val jobId = flint.refreshIndex(testIndex)
      val job = spark.streams.get(jobId.get)
      failAfter(streamingTimeout) {
        job.processAllAvailable()
      }

      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Update Flint index to incremental refresh
      val indexInitial = flint.describeIndex(testIndex).get
      val updatedIndex = flint
        .skippingIndex()
        .copyWithUpdate(
          indexInitial,
          FlintSparkIndexOptions(Map("auto_refresh" -> "false", "incremental_refresh" -> "true")))
      flint.updateIndex(updatedIndex) shouldBe empty

      // Expect refresh job to be stopped
      spark.streams.active.find(_.name == testIndex) shouldBe empty

      // Generate a new source file
      sql(s"""
             | INSERT INTO $testTable
             | PARTITION (year=2023, month=4)
             | VALUES ('Hello', 35, 'Vancouver')
             | """.stripMargin)

      // Index shouldn't be refreshed
      flint.queryIndex(testIndex).collect().toSet should have size 2

      // Delete all index data intentionally
      openSearchClient.deleteByQuery(
        new DeleteByQueryRequest(testIndex).setQuery(QueryBuilders.matchAllQuery()),
        RequestOptions.DEFAULT)

      // Expect to only refresh the new file
      flint.refreshIndex(testIndex) shouldBe empty
      flint.queryIndex(testIndex).collect().toSet should have size 1
    }
  }
}
