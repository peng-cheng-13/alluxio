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

/**
 * The Block index info.
 */
public class QueryInfo {
  private double mQueryMax;
  private double mQueryMin;
  private String mQueryVar = "";
  private boolean mAugmented;

  /**
   * Creates a new instance of {@link QueryInfo}.
   */
  public QueryInfo() {}

  /**
   * Creates a new instance of {@link QueryInfo} with init value.
   * @param max the query max value
   * @param min the query min value
   * @param var the query var name
   * @param augmented whether to use augmented index
   */
  public QueryInfo(double max, double min, String var, boolean augmented) {
    mQueryMax = max;
    mQueryMin = min;
    mQueryVar = var;
    mAugmented = augmented;
  }

  /**
   * Set max and min query value.
   * @param max the query max value
   * @param min the query min value
   */
  public void setMaxMin(double max, double min) {
    mQueryMax = max;
    mQueryMin = min;
  }

  /**
   * Set the query var name.
   * @param var the query var name
   */
  public void setVar(String var) {
    mQueryVar = var;
  }

  /**
   * Set Augmented index.
   * @param augmented whether to use augmented index
   */
  public void setAugmented(boolean augmented) {
    mAugmented = augmented;
  }

  /**
   * @return the query max value
   */
  public double getMaxValue() {
    return mQueryMax;
  }

  /**
   * @return the query min value
   */
  public double getMinValue() {
    return mQueryMin;
  }

  /**
   * @return the query var name
   */
  public String getVarName() {
    return mQueryVar;
  }

  /**
   * @return whether to use augmented index
   */
  public boolean useAugmented() {
    return mAugmented;
  }
}
