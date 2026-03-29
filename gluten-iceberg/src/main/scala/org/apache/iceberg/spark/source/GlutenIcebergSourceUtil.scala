/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.spark.source

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution.SparkDataSourceRDDPartition
import org.apache.gluten.substrait.rel.{IcebergLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType

import org.apache.iceberg._
import org.apache.iceberg.spark.SparkSchemaUtil

import java.lang.{Class, Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}
import java.util.Locale

import scala.collection.JavaConverters._

object GlutenIcebergSourceUtil {
  private val InputFileNameCol = "input_file_name"
  private val InputFileBlockStartCol = "input_file_block_start"
  private val InputFileBlockLengthCol = "input_file_block_length"

  def getClassOfSparkBatchQueryScan: Class[SparkBatchQueryScan] = {
    classOf[SparkBatchQueryScan]
  }

  private val SparkInputPartitionClassName = "org.apache.iceberg.spark.source.SparkInputPartition"

  def deleteExists(p: SparkDataSourceRDDPartition): Boolean = {
    p.inputPartitions.exists {
      case ip: SparkInputPartition =>
        // Fast path: same classloader.
        val tasks = ip.taskGroup[ScanTask]().tasks().asScala
        asFileScanTask(tasks.toList).exists(task => !task.deletes().isEmpty())
      case ip if ip.getClass.getName == SparkInputPartitionClassName =>
        // Slow path: classloader-isolated - invoke taskGroup().tasks() via reflection.
        val taskGroupObj = ip.getClass.getMethod("taskGroup").invoke(ip)
        val tasksObj = taskGroupObj.getClass
          .getMethod("tasks")
          .invoke(taskGroupObj)
          .asInstanceOf[java.lang.Iterable[_]]
        asFileScanTaskReflective(tasksObj.asScala.toList.map(_.asInstanceOf[AnyRef]))
          .exists {
            task =>
              !task.getClass
                .getMethod("deletes")
                .invoke(task)
                .asInstanceOf[java.util.List[_]]
                .isEmpty
          }
      case _ => throw new UnsupportedOperationException(s"Unsupported InputPartition type")
    }
  }

  def genSplitInfo(
      partition: SparkDataSourceRDDPartition,
      readPartitionSchema: StructType,
      metadataColumnNames: Seq[String]): SplitInfo = {
    val paths = new JArrayList[String]()
    val starts = new JArrayList[JLong]()
    val lengths = new JArrayList[JLong]()
    val partitionColumns = new JArrayList[JMap[String, String]]()
    val deleteFilesList = new JArrayList[JList[DeleteFile]]()
    val metadataColumns = new JArrayList[JMap[String, String]]()
    var fileFormat = ReadFileFormat.UnknownFormat

    partition.inputPartitions.foreach {
      case ip: SparkInputPartition =>
        // Fast path: same classloader - use typed API directly.
        val tasks = ip.taskGroup[ScanTask]().tasks().asScala
        asFileScanTask(tasks.toList).foreach {
          task =>
            val filePath = task.file().path().toString
            paths.add(BackendsApiManager.getTransformerApiInstance.encodeFilePathIfNeed(filePath))
            starts.add(task.start())
            lengths.add(task.length())
            partitionColumns.add(getPartitionColumns(task, readPartitionSchema))
            deleteFilesList.add(task.deletes())
            metadataColumns.add(
              genMetadataColumns(metadataColumnNames, filePath, task.start(), task.length()))
            val currentFileFormat = convertFileFormat(task.file().format())
            if (fileFormat == ReadFileFormat.UnknownFormat) {
              fileFormat = currentFileFormat
            } else if (fileFormat != currentFileFormat) {
              throw new UnsupportedOperationException(
                s"Only one file format is supported, " +
                  s"find different file format $fileFormat and $currentFileFormat")
            }
        }
      case ip if ip.getClass.getName == SparkInputPartitionClassName =>
        // Slow path: classloader-isolated - all Iceberg objects accessed via reflection.
        val taskGroupObj = ip.getClass.getMethod("taskGroup").invoke(ip)
        val rawTasks = taskGroupObj.getClass
          .getMethod("tasks")
          .invoke(taskGroupObj)
          .asInstanceOf[java.lang.Iterable[_]]
          .asScala
          .toList
          .map(_.asInstanceOf[AnyRef])
        asFileScanTaskReflective(rawTasks).foreach {
          task =>
            val fileObj = task.getClass.getMethod("file").invoke(task)
            val filePath = fileObj.getClass.getMethod("path").invoke(fileObj).toString
            val start = task.getClass.getMethod("start").invoke(task).asInstanceOf[Long]
            val length = task.getClass.getMethod("length").invoke(task).asInstanceOf[Long]
            paths.add(BackendsApiManager.getTransformerApiInstance.encodeFilePathIfNeed(filePath))
            starts.add(start)
            lengths.add(length)
            // Partition columns are derived from the already-resolved readPartitionSchema.
            partitionColumns.add(getPartitionColumnsReflective(task, readPartitionSchema))
            deleteFilesList.add(
              task.getClass
                .getMethod("deletes")
                .invoke(task)
                .asInstanceOf[JList[DeleteFile]])
            metadataColumns.add(genMetadataColumns(metadataColumnNames, filePath, start, length))
            val formatStr = fileObj.getClass.getMethod("format").invoke(fileObj).toString
            val currentFileFormat = formatStr match {
              case "PARQUET" => ReadFileFormat.ParquetReadFormat
              case "ORC" => ReadFileFormat.OrcReadFormat
              case _ =>
                throw new GlutenNotSupportException(
                  "Iceberg Only support parquet and orc file format.")
            }
            if (fileFormat == ReadFileFormat.UnknownFormat) {
              fileFormat = currentFileFormat
            } else if (fileFormat != currentFileFormat) {
              throw new UnsupportedOperationException(
                s"Only one file format is supported, " +
                  s"find different file format $fileFormat and $currentFileFormat")
            }
        }
      case o =>
        throw new GlutenNotSupportException(s"Unsupported input partition type: $o")
    }
    IcebergLocalFilesBuilder.makeIcebergLocalFiles(
      partition.index,
      paths,
      starts,
      lengths,
      partitionColumns,
      fileFormat,
      SoftAffinity
        .getFilePartitionLocations(paths.asScala.toArray, partition.preferredLocations())
        .toList
        .asJava,
      deleteFilesList,
      metadataColumns
    )
  }

  private def genMetadataColumns(
      metadataColumnNames: Seq[String],
      filePath: String,
      start: Long,
      length: Long): JHashMap[String, String] = {
    val metadataColumns = new JHashMap[String, String]()
    metadataColumnNames.foreach {
      name =>
        name.toLowerCase(Locale.ROOT) match {
          case InputFileNameCol => metadataColumns.put(name, filePath)
          case InputFileBlockStartCol => metadataColumns.put(name, start.toString)
          case InputFileBlockLengthCol => metadataColumns.put(name, length.toString)
          case _ =>
        }
    }
    metadataColumns
  }

  private val SparkBatchQueryScanClassName = "org.apache.iceberg.spark.source.SparkBatchQueryScan"

  // True when the scan's class is SparkBatchQueryScan by name, covering the case where EMR has
  // two Iceberg runtime jars on the classpath (iceberg-spark-runtime-3.5_2.12-*-amzn-0.jar and
  // iceberg-spark3-runtime.jar) that produce two distinct class objects for the same bytecode.
  // In that situation Scala pattern matching (which uses class identity) fails, so we fall back
  // to a reflection-based invocation path that does not require a cast.
  private def isSparkBatchQueryScan(scan: Scan): Boolean =
    scan.getClass.getName == SparkBatchQueryScanClassName

  // Invoke scan.tasks() via reflection and return the raw task objects.
  private def invokeTasksReflective(scan: Scan): List[AnyRef] = {
    scan.getClass
      .getMethod("tasks")
      .invoke(scan)
      .asInstanceOf[java.lang.Iterable[_]]
      .asScala
      .toList
      .map(_.asInstanceOf[AnyRef])
  }

  // Flatten to file-scan-task objects via reflection, mirroring asFileScanTask().
  private def asFileScanTaskReflective(rawTasks: List[AnyRef]): List[AnyRef] = {
    if (
      rawTasks.forall(t => t.getClass.getMethod("isFileScanTask").invoke(t).asInstanceOf[Boolean])
    ) {
      rawTasks.map(t => t.getClass.getMethod("asFileScanTask").invoke(t))
    } else {
      rawTasks.flatMap {
        t =>
          t.getClass
            .getMethod("tasks")
            .invoke(t)
            .asInstanceOf[java.lang.Iterable[_]]
            .asScala
            .map(_.asInstanceOf[AnyRef])
      }
    }
  }

  def getFileFormat(sparkScan: Scan): ReadFileFormat = sparkScan match {
    case scan: SparkBatchQueryScan =>
      // Fast path: same classloader - direct API access.
      val tasks = scan.tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          task.file().format() match {
            case FileFormat.PARQUET => return ReadFileFormat.ParquetReadFormat
            case FileFormat.ORC => return ReadFileFormat.OrcReadFormat
            case _ =>
          }
      }
      throw new GlutenNotSupportException("Iceberg Only support parquet and orc file format.")
    case _ if isSparkBatchQueryScan(sparkScan) =>
      // Slow path: classloader-isolated (e.g. dual Iceberg jars on EMR).
      // FileFormat enum's toString() returns the format name ("PARQUET", "ORC", ...).
      asFileScanTaskReflective(invokeTasksReflective(sparkScan)).foreach {
        task =>
          val fileObj = task.getClass.getMethod("file").invoke(task)
          val format = fileObj.getClass.getMethod("format").invoke(fileObj)
          format.toString match {
            case "PARQUET" => return ReadFileFormat.ParquetReadFormat
            case "ORC" => return ReadFileFormat.OrcReadFormat
            case _ =>
          }
      }
      throw new GlutenNotSupportException("Iceberg Only support parquet and orc file format.")
    case _ =>
      throw new GlutenNotSupportException("Only support iceberg SparkBatchQueryScan.")
  }

  def getReadPartitionSchema(sparkScan: Scan): StructType = sparkScan match {
    case scan: SparkBatchQueryScan =>
      // Fast path: same classloader - direct API access.
      val tasks = scan.tasks().asScala
      asFileScanTask(tasks.toList).foreach {
        task =>
          val spec = task.spec()
          if (spec.isPartitioned) {
            val readFields = scan.readSchema().fields.map(_.name).toSet
            // Iceberg will generate some non-table fields as partition fields, such as x_bucket,
            // which will not appear in readFields, they also cannot be filtered.
            val tableFields = spec.schema().columns().asScala.map(_.name()).toSet
            val voidTransformFields = scan
              .table()
              .spec()
              .fields()
              .asScala
              .filter(
                f => {
                  f.transform().isVoid
                })
              .map(_.name())
              .toSet
            val partitionFields =
              spec
                .partitionType()
                .fields()
                .asScala
                .filter(f => !tableFields.contains(f.name) || readFields.contains(f.name()))
                .filter(f => !voidTransformFields.contains(f.name()))
            partitionFields.foreach {
              field => TypeUtil.validatePartitionColumnType(field.`type`().typeId())
            }

            val icebergSchema = new Schema(partitionFields.toList.asJava)
            return SparkSchemaUtil.convert(icebergSchema)
          } else {
            return new StructType()
          }
      }
      throw new UnsupportedOperationException(
        "Failed to get partition schema from iceberg SparkBatchQueryScan.")
    case _ if isSparkBatchQueryScan(sparkScan) =>
      // Slow path: classloader-isolated. All Iceberg objects are invoked via reflection so we
      // never cast to a class loaded by a different classloader.
      // readSchema() is defined on the Scan interface (Spark type) - safe to call directly.
      val readFields = sparkScan.readSchema().fields.map(_.name).toSet
      val fileTasks = asFileScanTaskReflective(invokeTasksReflective(sparkScan))
      if (fileTasks.isEmpty) {
        throw new UnsupportedOperationException(
          "Failed to get partition schema from iceberg SparkBatchQueryScan.")
      }
      val spec = fileTasks.head.getClass.getMethod("spec").invoke(fileTasks.head)
      val isPartitioned = spec.getClass
        .getMethod("isPartitioned")
        .invoke(spec)
        .asInstanceOf[Boolean]
      if (!isPartitioned) {
        return new StructType()
      }
      // Derive void-transform field names from table spec.
      val scan = sparkScan
      val tableObj = scan.getClass.getMethod("table").invoke(scan)
      val tableSpec = tableObj.getClass.getMethod("spec").invoke(tableObj)
      val tableSpecFieldsList = tableSpec.getClass
        .getMethod("fields")
        .invoke(tableSpec)
        .asInstanceOf[java.util.List[_]]
        .asScala
      val voidTransformFields = tableSpecFieldsList
        .filter {
          f =>
            val t = f.getClass.getMethod("transform").invoke(f)
            t.getClass.getMethod("isVoid").invoke(t).asInstanceOf[Boolean]
        }
        .map(f => f.getClass.getMethod("name").invoke(f).asInstanceOf[String])
        .toSet

      val specSchema = spec.getClass.getMethod("schema").invoke(spec)
      val tableFields = specSchema.getClass
        .getMethod("columns")
        .invoke(specSchema)
        .asInstanceOf[java.util.List[_]]
        .asScala
        .map(c => c.getClass.getMethod("name").invoke(c).asInstanceOf[String])
        .toSet

      val partitionType = spec.getClass.getMethod("partitionType").invoke(spec)
      val allPartitionFields = partitionType.getClass
        .getMethod("fields")
        .invoke(partitionType)
        .asInstanceOf[java.util.List[_]]
        .asScala
        .toList

      val filteredFields = allPartitionFields.filter {
        f =>
          val name = f.getClass.getMethod("name").invoke(f).asInstanceOf[String]
          (!tableFields.contains(name) || readFields.contains(name)) &&
          !voidTransformFields.contains(name)
      }

      // Build an Iceberg Schema from the filtered NestedField objects and convert via
      // SparkSchemaUtil - both called through the scan's own classloader so that the
      // argument types are compatible.
      val scanCL = sparkScan.getClass.getClassLoader
      val schemaClass = scanCL.loadClass("org.apache.iceberg.Schema")
      val listCtor = schemaClass.getConstructor(classOf[java.util.List[_]])
      val fieldList = new java.util.ArrayList[AnyRef]()
      filteredFields.foreach(f => fieldList.add(f.asInstanceOf[AnyRef]))
      val icebergSchema = listCtor.newInstance(fieldList).asInstanceOf[AnyRef]
      val sparkSchemaUtilClass = scanCL.loadClass("org.apache.iceberg.spark.SparkSchemaUtil")
      sparkSchemaUtilClass
        .getMethod("convert", schemaClass)
        .invoke(null, icebergSchema)
        .asInstanceOf[StructType]
    case _ =>
      throw new UnsupportedOperationException("Only support iceberg SparkBatchQueryScan.")
  }

  private def asFileScanTask(tasks: List[ScanTask]): List[FileScanTask] = {
    if (tasks.forall(_.isFileScanTask)) {
      tasks.map(_.asFileScanTask())
    } else if (tasks.forall(_.isInstanceOf[CombinedScanTask])) {
      tasks.flatMap(_.asCombinedScanTask().tasks().asScala)
    } else {
      throw new UnsupportedOperationException(
        "Only support iceberg CombinedScanTask and FileScanTask.")
    }
  }

  private def getPartitionColumns(
      task: FileScanTask,
      readPartitionSchema: StructType): JHashMap[String, String] = {
    val partitionColumns = new JHashMap[String, String]()
    val readPartitionFields = readPartitionSchema.fields.map(_.name).toSet
    val spec = task.spec()
    val partition = task.partition()
    if (spec.isPartitioned) {
      val partitionFields = spec
        .partitionType()
        .fields()
        .asScala
        .zipWithIndex
        .filter(f => readPartitionFields.contains(f._1.name()))
      partitionFields.foreach {
        case (field, index) =>
          val partitionValue = partition.get(index, field.`type`().typeId().javaClass())
          val partitionType = field.`type`()
          if (partitionValue != null) {
            partitionColumns.put(
              field.name(),
              TypeUtil.getPartitionValueString(partitionType, partitionValue))
          } else {
            partitionColumns.put(field.name(), ExternalCatalogUtils.DEFAULT_PARTITION_NAME)
          }
      }
    }
    partitionColumns
  }

  // Reflection-based equivalent of getPartitionColumns for classloader-isolated FileScanTask.
  private def getPartitionColumnsReflective(
      task: AnyRef,
      readPartitionSchema: StructType): JHashMap[String, String] = {
    val partitionColumns = new JHashMap[String, String]()
    val readPartitionFields = readPartitionSchema.fields.map(_.name).toSet
    if (readPartitionFields.isEmpty) return partitionColumns

    val spec = task.getClass.getMethod("spec").invoke(task)
    val isPartitioned = spec.getClass
      .getMethod("isPartitioned")
      .invoke(spec)
      .asInstanceOf[Boolean]
    if (!isPartitioned) return partitionColumns

    val partition = task.getClass.getMethod("partition").invoke(task)
    val partitionTypeObj = spec.getClass.getMethod("partitionType").invoke(spec)
    val partitionFields = partitionTypeObj.getClass
      .getMethod("fields")
      .invoke(partitionTypeObj)
      .asInstanceOf[java.util.List[_]]
      .asScala
      .toList

    partitionFields.zipWithIndex.foreach {
      case (field, index) =>
        val name = field.getClass.getMethod("name").invoke(field).asInstanceOf[String]
        if (readPartitionFields.contains(name)) {
          val fieldType = field.getClass.getMethod("type").invoke(field)
          val typeId = fieldType.getClass.getMethod("typeId").invoke(fieldType)
          val javaClass = typeId.getClass
            .getMethod("javaClass")
            .invoke(typeId)
            .asInstanceOf[Class[_]]
          val partitionValue = partition.getClass
            .getMethod("get", classOf[Int], classOf[Class[_]])
            .invoke(partition, index.asInstanceOf[AnyRef], javaClass)
          if (partitionValue != null) {
            partitionColumns.put(
              name,
              TypeUtil.getPartitionValueStringReflective(fieldType, partitionValue))
          } else {
            partitionColumns.put(name, ExternalCatalogUtils.DEFAULT_PARTITION_NAME)
          }
        }
    }
    partitionColumns
  }

  private def convertFileFormat(icebergFileFormat: FileFormat): ReadFileFormat =
    icebergFileFormat match {
      case FileFormat.PARQUET => ReadFileFormat.ParquetReadFormat
      case FileFormat.ORC => ReadFileFormat.OrcReadFormat
      case _ =>
        throw new GlutenNotSupportException("Iceberg Only support parquet and orc file format.")
    }
}
