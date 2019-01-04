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

package alluxio.wire;

import java.io.Serializable;

/**
 * The Block index info.
 */
public class IndexInfo implements Serializable {
  private static final long serialVersionUID = 1119966306934831779L;

  private long mBlockId;
  private double mMaxValue;
  private double mMinValue;
  private String mVarName = "";
  private long mBitmap;

  /**
   * Creates a new instance of {@link IndexInfo}.
   */
  public IndexInfo() {}

  /**
   * Creates a new instance of {@link IndexInfo} with init value.
   * @param id the block id
   * @param max the block max value
   * @param min the block min value
   * @param var the index var name
   */
  public IndexInfo(long id, double max, double min, String var) {
    mBlockId = id;
    mMaxValue = max;
    mMinValue = min;
    mVarName = var;
    mBitmap = 0xFFFF;
  }

  /**
   * Set the BlockId.
   * @param bid the BlockId
   */
  public void setBlockId(long bid) {
    mBlockId = bid;
  }

  /**
   * Set the MinMax value.
   * @param min the min value
   * @param max the max value
   */
  public void setMinMax(double min, double max) {
    mMinValue = min;
    mMaxValue = max;
  }

  /**
   * Set the var name.
   * @param vname the var name
   */
  public void setVarName(String vname) {
    mVarName = vname;
  }

  /**
   * Set the augmented bitmap.
   * @param tmpbitmap the current bitmap
   */
  public void setBitmap(long tmpbitmap) {
    mBitmap = tmpbitmap;
  }

  /**
   * @return the block ID
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * @return the block min value
   */
  public double getMaxValue() {
    return mMaxValue;
  }

  /**
   * @return the block max value
   */
  public double getMinValue() {
    return mMinValue;
  }

  /**
   * @return the name of index var
   */
  public String getVarName() {
    return mVarName;
  }

  /**
   * Get the augmented bitmap.
   * @return current bitmap
   */
  public long getBitmap() {
    return mBitmap;
  }

}
