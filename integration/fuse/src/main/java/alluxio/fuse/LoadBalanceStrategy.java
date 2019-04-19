/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse;

import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.meta.StorageTier;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.WorkerOutOfSpaceException;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
final class LoadBalanceStrategy {
  private static int sNum = 0;
  private static int sI = 0;
  private static StorageTier sTier0;
  private static StorageTier sTier1;

  public LoadBalanceStrategy() {
  }

  public static int MaxFree() throws BlockAlreadyExistsException, IOException,
      WorkerOutOfSpaceException {
    BlockMetadataManager mMetaManager = BlockMetadataManager.createBlockMetadataManager();
    sTier0 = mMetaManager.getTier("MEM");
    sTier1 = mMetaManager.getTier("HDD");
    long freeTier0 = sTier0.getAvailableBytes();
    long avaTier0 = sTier0.getCapacityBytes();
    long freeTier1 = sTier1.getAvailableBytes();
    long avaTier1 = sTier1.getCapacityBytes();
    double f0 = ((double) freeTier0) / ((double) avaTier0);
    double f1 = ((double) freeTier1) / ((double) avaTier1);
    if (f0 < f1) {
      return 1;
    } else {
      return 0;
    }
  }

  public static int Random() {
    sI = (int) (Math.random() * 2);
    return sI;
  }

  public static int RoundRobin() {
    sNum++;
    if (sNum == 1000) {
      sNum = 0;
    }
    sI = (sNum - 1) % 2;
    return sI;
  }
}

