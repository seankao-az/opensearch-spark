/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

case class FlintPartitionReaderFactory(
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate])
    extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val query = FlintQueryCompiler(schema).compile(pushedPredicates)
    new FlintPartitionReader(
      partition.asInstanceOf[OpenSearchSplit].table.createReader(query),
      schema,
      options)
  }
}
