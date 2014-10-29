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

package org.apache.spark.ui.storage

import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ui._
import org.apache.spark.scheduler._
import org.apache.spark.storage._

/** Web UI showing storage status of all RDD's in the given SparkContext. */
private[ui] class StorageTab(parent: SparkUI) extends SparkUITab(parent, "storage") {
  val listener = parent.storageListener

  attachPage(new StoragePage(this))
  attachPage(new RDDPage(this))
  attachPage(new BroadcastPage(this))
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the BlockManagerUI.
 */
@DeveloperApi
class StorageListener(storageStatusListener: StorageStatusListener) extends SparkListener {
  private[ui] val _rddInfoMap = mutable.Map[Int, RDDInfo]() // exposed for testing
  private[ui] val _broadcastInfoMap = mutable.Map[Long, BroadcastInfo]()

  def storageStatusList = storageStatusListener.storageStatusList

  /** Filter RDD info to include only those with cached partitions */
  def rddInfoList = _rddInfoMap.values.filter(_.numCachedPartitions > 0).toSeq

  /** Filter broadcast info to include only those with cached partitions */
  def broadcastInfoList = _broadcastInfoMap.values.toSeq

  /** Update the storage info of the RDDs whose blocks are among the given updated blocks */
  private def updateRDDInfo(updatedBlocks: Seq[(BlockId, BlockStatus)]): Unit = {
    val rddIdsToUpdate = updatedBlocks.flatMap { case (bid, _) => bid.asRDDId.map(_.rddId) }.toSet
    val rddInfosToUpdate = _rddInfoMap.values.toSeq.filter { s => rddIdsToUpdate.contains(s.id) }
    StorageUtils.updateRddInfo(rddInfosToUpdate, storageStatusList)
  }

  /**
   * Assumes the storage status list is fully up-to-date. This implies the corresponding
   * StorageStatusSparkListener must process the SparkListenerTaskEnd event before this listener.
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val metrics = taskEnd.taskMetrics
    if (metrics != null && metrics.updatedBlocks.isDefined) {
      updateRDDInfo(metrics.updatedBlocks.get)
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = synchronized {
    val rddInfos = stageSubmitted.stageInfo.rddInfos
    rddInfos.foreach { info => _rddInfoMap.getOrElseUpdate(info.id, info) }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = synchronized {
    // Remove all partitions that are no longer cached in current completed stage
    val completedRddIds = stageCompleted.stageInfo.rddInfos.map(r => r.id).toSet
    _rddInfoMap.retain { case (id, info) =>
      !completedRddIds.contains(id) || info.numCachedPartitions > 0
    }
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) = synchronized {
    _rddInfoMap.remove(unpersistRDD.rddId)
  }

  override def onBlockUpdate(blockUpdateEvent: SparkListenerBlockUpdate) = synchronized {
    // only update broadcast for now, need to be modified if want to track other blocks
    val broadcastId = blockUpdateEvent.blockId.asBroadcastId.get.broadcastId
    val blocksToUpdate = blockUpdateEvent.blockStatus
    val broadcastInfoToUpdate = _broadcastInfoMap.getOrElseUpdate(
      broadcastId, new BroadcastInfo(broadcastId, "broadcast_%d".format(broadcastId), 0))
    println("updating the broadcast variable:" +
      blockUpdateEvent.blockId.asBroadcastId.get.broadcastId)
    StorageUtils.updateBroadcastInfo(broadcastInfoToUpdate, storageStatusList)
    println("current broadcastInfoMap:" + _broadcastInfoMap)
    if (broadcastInfoToUpdate.memSize == 0 && broadcastInfoToUpdate.diskSize == 0 &&
      broadcastInfoToUpdate.tachyonSize == 0) {
      _broadcastInfoMap.remove(broadcastId)
    }
  }
}
