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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.spark.sql.{sources, Strategy}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, Expression, ExprId, IsNotNull, IsNull, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, ProjectionOverSchema, SelectedField}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, Repartition}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaPruning.RootField
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader
import org.apache.spark.sql.types._

object DataSourceV2Strategy extends Strategy {

  /**
   * Pushes down filters to the data source reader
   *
   * @return pushed filter and post-scan filters.
   */
  private def pushFilters(
      reader: DataSourceReader,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    reader match {
      case r: SupportsPushDownFilters =>
        // A map from translated data source filters to original catalyst filter expressions.
        val translatedFilterToExpr = mutable.HashMap.empty[sources.Filter, Expression]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated = DataSourceStrategy.translateFilter(filterExpr)
          if (translated.isDefined) {
            translatedFilterToExpr(translated.get) = filterExpr
          } else {
            untranslatableExprs += filterExpr
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters = r.pushFilters(translatedFilterToExpr.keys.toArray)
          .map(translatedFilterToExpr)
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map(translatedFilterToExpr)
        (pushedFilters, untranslatableExprs ++ postScanFilters)

      case _ => (Nil, filters)
    }
  }

  /**
   * Gets the root (aka top-level, no-parent) [[StructField]]s for the given [[Expression]].
   * When expr is an [[Attribute]], construct a field around it and indicate that that
   * field was derived from an attribute.
   */
  private def getRootFields(expr: Expression): Seq[RootField] = {
    expr match {
      case att: Attribute =>
        RootField(StructField(att.name, att.dataType, att.nullable), derivedFromAtt = true) :: Nil
      case SelectedField(field) =>
        RootField(field, derivedFromAtt = false) :: Nil
      // Root field accesses by `IsNotNull` and `IsNull` are special cases as the expressions
      // don't actually use any nested fields. These root field accesses might be excluded later
      // if there are any nested fields accesses in the query plan.
      case IsNotNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, contentAccessed = false) :: Nil
      case IsNull(SelectedField(field)) =>
        RootField(field, derivedFromAtt = false, contentAccessed = false) :: Nil
      case IsNotNull(_: Attribute) | IsNull(_: Attribute) =>
        expr.children.flatMap(getRootFields).map(_.copy(contentAccessed = false))
      case _ =>
        expr.children.flatMap(getRootFields)
    }
  }

  /**
   * Returns the set of fields from the Parquet file that the query plan needs.
   */
  private def identifyRootFields(expressions: Seq[Expression]) = {

    val rootFields = expressions.flatMap(field => getRootFields(field))

    // Kind of expressions don't need to access any fields of a root fields, e.g., `IsNotNull`.
    // For them, if there are any nested fields accessed in the query, we don't need to add root
    // field access of above expressions.
    // For example, for a query `SELECT name.first FROM contacts WHERE name IS NOT NULL`,
    // we don't need to read nested fields of `name` struct other than `first` field.
    val (necessaryRootFields, optRootFields) = rootFields.distinct.partition(_.contentAccessed)

    optRootFields.filter { opt =>
      !necessaryRootFields.exists(_.field.name == opt.field.name)
    } ++ necessaryRootFields
  }

  /**
   * Counts the "leaf" fields of the given dataType. Informally, this is the
   * number of fields of non-complex data type in the tree representation of
   * [[DataType]].
   */
  private def countLeaves(dataType: DataType): Int = {
    dataType match {
      case array: ArrayType => countLeaves(array.elementType)
      case map: MapType => countLeaves(map.keyType) + countLeaves(map.valueType)
      case struct: StructType =>
        struct.map(field => countLeaves(field.dataType)).sum
      case _ => 1
    }
  }

  /**
   * Applies column pruning to the data source, w.r.t. the references of the given expressions.
   *
   * @return new output attributes after column pruning.
   */
  // TODO: nested column pruning.
  private def pruneColumns(
      reader: DataSourceReader,
      relation: DataSourceV2Relation,
      exprs: Seq[Expression]): Seq[AttributeReference] = {
    // scalastyle:off
    reader match {
      case r: SupportsPushDownRequiredColumns =>
        // val requiredColumns = AttributeSet(exprs.flatMap(_.references))
        val requestedRootFields = identifyRootFields(exprs)
        if (requestedRootFields.exists { case RootField(_, derivedFromAtt, _) =>
          !derivedFromAtt }) {
          val prunedSchema = requestedRootFields
            .map { case RootField(field, _, _) => StructType(Array(field)) }
            .reduceLeft(_ merge _)
          if (relation.userSpecifiedSchema.isEmpty ||
            (countLeaves(relation.schema) > countLeaves(prunedSchema))) {
            println("prunedSchema1:")
            prunedSchema.printTreeString()
            val projectionOverSchema = ProjectionOverSchema(prunedSchema)
            val requestedColumns = exprs.map(_.transformDown {
              case projectionOverSchema(expr) => expr
            })
            println(s"requestedColumns:\n" + requestedColumns.map(_.treeString).mkString("\n"))
            val referredAtts = requestedColumns.flatMap(_.references).distinct
            println(s"referredAtt: ${referredAtts.mkString(",")}")
            println(s"relation output: ${relation.output.mkString(",")}")
            val neededOutput = relation.output.filter(referredAtts.contains)
            if (neededOutput != relation.output) {
              r.pruneColumns(neededOutput.toStructType)
              val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
              r.readSchema().toAttributes.map {
                // We have to keep the attribute id during transformation.
                a => a.withExprId(nameToAttr(a.name).exprId)
              }
            } else {
              relation.output
            }
          } else {
            relation.output
          }
        } else {
          relation.output
        }
      case _ => relation.output
    }
  }

  /**
   * Normalizes the names of the attribute references in the given projects and filters to reflect
   * the names in the given logical relation. This makes it possible to compare attributes and
   * fields by name. Returns a tuple with the normalized projects and filters, respectively.
   */
  private def normalizeAttributeRefNames(
      normalizedAttNameMap: Map[ExprId, String],
      projects: Seq[NamedExpression],
      filters: Seq[Expression]): (Seq[NamedExpression], Seq[Expression]) = {
    val normalizedProjects = projects.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    }).map { case expr: NamedExpression => expr }
    val normalizedFilters = filters.map(_.transform {
      case att: AttributeReference if normalizedAttNameMap.contains(att.exprId) =>
        att.withName(normalizedAttNameMap(att.exprId))
    })
    (normalizedProjects, normalizedFilters)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
      val reader = relation.newReader()
      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFilters) = pushFilters(reader, filters)
      val output = pruneColumns(reader, relation, project ++ pushedFilters)
      logInfo(
        s"""
           |Pushing operators to ${relation.source.getClass}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val scan = DataSourceV2ScanExec(
        output, relation.source, relation.options, pushedFilters, reader)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

      // always add the projection, which will produce unsafe rows required by some operators
      ProjectExec(project, withFilter) :: Nil

    case r: StreamingDataSourceV2Relation =>
      // ensure there is a projection, which will produce unsafe rows required by some operators
      ProjectExec(r.output,
        DataSourceV2ScanExec(r.output, r.source, r.options, r.pushedFilters, r.reader)) :: Nil

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case AppendData(r: DataSourceV2Relation, query, _) =>
      WriteToDataSourceV2Exec(r.newWriter(), planLater(query)) :: Nil

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case Repartition(1, false, child) =>
      val isContinuous = child.collectFirst {
        case StreamingDataSourceV2Relation(_, _, _, r: ContinuousReader) => r
      }.isDefined

      if (isContinuous) {
        ContinuousCoalesceExec(1, planLater(child)) :: Nil
      } else {
        Nil
      }

    case _ => Nil
  }
}
