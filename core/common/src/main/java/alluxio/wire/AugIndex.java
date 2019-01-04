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
public class AugIndex {
  public String mVarName;
  public double mMin;
  public double mRange;

  /**
   * Creates a new instance of {@link AugIndex}.
   */
  public AugIndex() {}

  /**
   * Creates a new instance of {@link IndexInfo} with init value.
   * @param var the index var name
   * @param min the block min value
   * @param range the block range
   */
  public AugIndex(String var, double min, double range) {
    mVarName = var;
    mMin = min;
    mRange = range;
  }
}
