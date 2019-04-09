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

import alluxio.thrift.H5DatasetInfo;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

/**
 * The Data Structure handles hdf5 dataset.
 */
public class HDFDataSet implements Serializable {
  private static final long serialVersionUID = 1119967306934851800L;
  public String mName;
  public long mSize;
  public String mDataType;
  public Map<String, String> mAttribute = new HashMap<>();

  /**
   * Create a new instance of HDFDataSet.
   * @param name the name of the dataset
   */
  public HDFDataSet(String name) {
    mName = name;
    mSize = -1;
    mDataType = "N/A";
  }

  /**
   * Create a new instance of HDFDataSet.
   * @param name the name of the dataset
   * @param size the size of the dataset
   * @param datatype the datatype of the dataset
   */
  public HDFDataSet(String name, long size, String datatype) {
    mName = name;
    mSize = size;
    mDataType = datatype;
  }

  /**
   * Add attributes to HDFDataSet.
   * @param attr the input attributes
   */
  public void setUDM(Map<String, String> attr) {
    mAttribute = attr;
  }

  /**
   * @return name of the HDFDataSet
   */
  public String getName() {
    return mName;
  }

  /**
   * @return size of the HDFDataSet
   */
  public long getSize() {
    return mSize;
  }

  /**
   * @return data type of the HDFDataSet
   */
  public String getType() {
    return mDataType;
  }

  /**
   * @return attributes of the HDFDataSet
   */
  public Map<String, String> getUDM() {
    return mAttribute;
  }

  /**
   * @return Thrift representation of H5DataSet
   */
  public H5DatasetInfo toThrift() {
    H5DatasetInfo info = new H5DatasetInfo(mName, mSize, mDataType, mAttribute);
    return info;
  }
}
