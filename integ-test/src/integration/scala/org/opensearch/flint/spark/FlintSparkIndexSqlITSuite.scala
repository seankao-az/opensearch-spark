/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.collection.JavaConverters._

import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.opensearch.flint.spark.FlintSparkIndexOptions.OptionName.AUTO_REFRESH
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.Row

class FlintSparkIndexSqlITSuite extends FlintSparkSuite with Matchers {

  private val testTableName = "index_test"
  private val testTableQualifiedName = s"spark_catalog.default.$testTableName"
  private val testCoveringIndex = "name_age"
  private val testMvIndexShortName = "mv1"
  private val testMvQuery = s"SELECT name, age FROM $testTableQualifiedName"

  private val testSkippingFlintIndex =
    FlintSparkSkippingIndex.getSkippingIndexName(testTableQualifiedName)
  private val testCoveringFlintIndex =
    FlintSparkCoveringIndex.getFlintIndexName(testCoveringIndex, testTableQualifiedName)
  private val testMvIndex = s"spark_catalog.default.$testMvIndexShortName"
  private val testMvFlintIndex = FlintSparkMaterializedView.getFlintIndexName(testMvIndex)

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTimeSeriesTable(testTableQualifiedName)
  }

  test("show all flint indexes in catalog and database") {
    // Show in catalog
    flint
      .materializedView()
      .name(testMvIndex)
      .query(testMvQuery)
      .create()

    flint
      .coveringIndex()
      .name(testCoveringIndex)
      .onTable(testTableQualifiedName)
      .addIndexColumns("name", "age")
      .create()

    flint
      .skippingIndex()
      .onTable(testTableQualifiedName)
      .addValueSet("name")
      .create()

    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog"),
      Seq(
        Row(testMvFlintIndex, "mv", "default", null, testMvIndexShortName, false, "active"),
        Row(
          testCoveringFlintIndex,
          "covering",
          "default",
          testTableName,
          testCoveringIndex,
          false,
          "active"),
        Row(testSkippingFlintIndex, "skipping", "default", testTableName, null, false, "active")))

    // Create index in other database
    flint
      .materializedView()
      .name("spark_catalog.other.mv2")
      .query(testMvQuery)
      .create()

    // Show in catalog.database shouldn't show index in other database
    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog.default"),
      Seq(
        Row(testMvFlintIndex, "mv", "default", null, testMvIndexShortName, false, "active"),
        Row(
          testCoveringFlintIndex,
          "covering",
          "default",
          testTableName,
          testCoveringIndex,
          false,
          "active"),
        Row(testSkippingFlintIndex, "skipping", "default", testTableName, null, false, "active")))

    deleteTestIndex(
      testMvFlintIndex,
      testCoveringFlintIndex,
      testSkippingFlintIndex,
      FlintSparkMaterializedView.getFlintIndexName("spark_catalog.other.mv2"))
  }

  test("show flint indexes with extended information") {
    // Create and refresh with all existing data
    flint
      .skippingIndex()
      .onTable(testTableQualifiedName)
      .addValueSet("name")
      .options(
        FlintSparkIndexOptions(Map(AUTO_REFRESH.toString -> "true")),
        testSkippingFlintIndex)
      .create()
    flint.refreshIndex(testSkippingFlintIndex)
    val activeJob = spark.streams.active.find(_.name == testSkippingFlintIndex)
    awaitStreamingComplete(activeJob.get.id.toString)

    // Assert output contains empty error message
    def outputError: String = {
      val df = sql("SHOW FLINT INDEX EXTENDED IN spark_catalog")
      df.columns should contain("error")
      df.collect().head.getAs[String]("error")
    }
    outputError shouldBe empty

    // Trigger next micro batch after 5 seconds with index readonly
    new Thread(() => {
      Thread.sleep(5000)
      openSearchClient
        .indices()
        .putSettings(
          new UpdateSettingsRequest(testSkippingFlintIndex).settings(
            Map("index.blocks.write" -> true).asJava),
          RequestOptions.DEFAULT)
      sql(
        s"INSERT INTO $testTableQualifiedName VALUES (TIMESTAMP '2023-10-01 04:00:00', 'F', 25, 'Vancouver')")
    }).start()

    // Await to store exception and verify if it's as expected
    flint.flintIndexMonitor.awaitMonitor(Some(testSkippingFlintIndex))
    outputError should include("OpenSearchException")

    deleteTestIndex(testSkippingFlintIndex)
  }

  test("should return empty when show flint index in empty database") {
    checkAnswer(sql(s"SHOW FLINT INDEX IN spark_catalog.default"), Seq.empty)
  }

  test("show flint index with auto refresh") {
    flint
      .coveringIndex()
      .name(testCoveringIndex)
      .onTable(testTableQualifiedName)
      .addIndexColumns("name", "age")
      .options(
        FlintSparkIndexOptions(Map(AUTO_REFRESH.toString -> "true")),
        testCoveringFlintIndex)
      .create()
    flint.refreshIndex(testCoveringFlintIndex)

    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog"),
      Seq(
        Row(
          testCoveringFlintIndex,
          "covering",
          "default",
          testTableName,
          testCoveringIndex,
          true,
          "refreshing")))
    deleteTestIndex(testCoveringFlintIndex)
  }

  test("show flint index in database with the same prefix") {
    flint.materializedView().name("spark_catalog.default.mv1").query(testMvQuery).create()
    flint.materializedView().name("spark_catalog.default_test.mv2").query(testMvQuery).create()
    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog.default").select("index_name"),
      Seq(Row("mv1")))

    deleteTestIndex(
      FlintSparkMaterializedView.getFlintIndexName("spark_catalog.default.mv1"),
      FlintSparkMaterializedView.getFlintIndexName("spark_catalog.default_test.mv2"))
  }

  test("should ignore non-Flint index") {
    try {
      sql(s"CREATE SKIPPING INDEX ON $testTableQualifiedName (name VALUE_SET)")

      // Create a non-Flint index which has "flint_" prefix in coincidence
      openSearchClient
        .indices()
        .create(
          new CreateIndexRequest("flint_spark_catalog_invalid_index1"),
          RequestOptions.DEFAULT)

      // Create a non-Flint index which has "flint_" prefix and _meta mapping in coincidence
      openSearchClient
        .indices()
        .create(
          new CreateIndexRequest("flint_spark_catalog_invalid_index2")
            .mapping(
              """{
                |  "_meta": {
                |    "custom": "test"
                |  }
                |}
                |""".stripMargin,
              XContentType.JSON),
          RequestOptions.DEFAULT)

      // Show statement should ignore such index without problem
      checkAnswer(
        sql(s"SHOW FLINT INDEX IN spark_catalog"),
        Row(testSkippingFlintIndex, "skipping", "default", testTableName, null, false, "active"))
    } finally {
      deleteTestIndex(testSkippingFlintIndex)
    }
  }

  test("show flint index with special characters") {
    val testCoveringIndexSpecial = "test :\"+/\\|?#><"
    val testCoveringFlintIndexSpecial =
      FlintSparkCoveringIndex.getFlintIndexName(testCoveringIndexSpecial, testTableQualifiedName)

    flint
      .coveringIndex()
      .name(testCoveringIndexSpecial)
      .onTable(testTableQualifiedName)
      .addIndexColumns("name", "age")
      .options(
        FlintSparkIndexOptions(Map(AUTO_REFRESH.toString -> "true")),
        testCoveringFlintIndexSpecial)
      .create()
    flint.refreshIndex(testCoveringFlintIndexSpecial)

    checkAnswer(
      sql(s"SHOW FLINT INDEX IN spark_catalog"),
      Seq(
        Row(
          testCoveringFlintIndexSpecial,
          "covering",
          "default",
          testTableName,
          testCoveringIndexSpecial,
          true,
          "refreshing")))
    deleteTestIndex(testCoveringFlintIndexSpecial)
  }
}
