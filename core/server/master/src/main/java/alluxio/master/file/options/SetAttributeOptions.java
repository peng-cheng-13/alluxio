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

package alluxio.master.file.options;

import alluxio.Constants;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.thrift.H5DatasetInfo;
import alluxio.wire.HDFDataSet;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting the attributes.
 */
@NotThreadSafe
public final class SetAttributeOptions {
  private Boolean mPinned;
  private Long mTtl;
  private TtlAction mTtlAction;
  private Boolean mPersisted;
  private String mOwner;
  private String mGroup;
  private Short mMode;
  private boolean mRecursive;
  private long mOperationTimeMs;
  public boolean mShouldindex;
  private List<Long> mBlockIndexBid = new ArrayList<Long>();
  private List<Double> mBlockIndexMax = new ArrayList<Double>();
  private List<Double> mBlockIndexMin = new ArrayList<Double>();
  private List<String> mBlockIndexVar = new ArrayList<String>();
  private List<Long> mBlockAugIndex = new ArrayList<Long>();
  public boolean mUDM;
  private List<String> mPath = new ArrayList<String>();
  private List<String> mUKey = new ArrayList<String>();
  private List<String> mUValue = new ArrayList<String>();
  public boolean mDeleteAttribute;
  public boolean mHasH5Dataset = false;
  public ArrayList<HDFDataSet> mH5Dateset = new ArrayList<>();

  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new SetAttributeOptions();
  }

  /**
   * Constructs a new method option for setting the attributes.
   *
   * @param options the options for setting the attributes
   */
  public SetAttributeOptions(SetAttributeTOptions options) {
    mPinned = options.isSetPinned() ? options.isPinned() : null;
    mTtl = options.isSetTtl() ? options.getTtl() : null;
    mTtlAction = ThriftUtils.fromThrift(options.getTtlAction());
    mPersisted = options.isSetPersisted() ? options.isPersisted() : null;
    mOwner = options.isSetOwner() ? options.getOwner() : null;
    mGroup = options.isSetGroup() ? options.getGroup() : null;
    mMode = options.isSetMode() ? options.getMode() : Constants.INVALID_MODE;
    mRecursive = options.isRecursive();
    mOperationTimeMs = System.currentTimeMillis();
    if (options.getMIndexInfo_BlockId() != null) {
      mShouldindex = true;
      mBlockIndexBid = options.getMIndexInfo_BlockId();
      mBlockIndexMax = options.getMIndexInfo_MaxValue();
      mBlockIndexMin = options.getMIndexInfo_MinValue();
      mBlockIndexVar = options.getMIndexInfo_VarName();
      mBlockAugIndex = options.getMAugIndex();
    }
    if (options.getMUKey() != null) {
      mUDM = true;
      mPath = options.getMPath();
      mUKey = options.getMUKey();
      mUValue = options.getMUValue();
      mDeleteAttribute = options.mDeleteAttribute;
    }
    if (options.isSetDataset()) {
      mHasH5Dataset = true;
      List<H5DatasetInfo> datasetinfo = options.getDataset();
      H5DatasetInfo tmpinfo;
      HDFDataSet tmp;
      for (int i = 0; i < datasetinfo.size(); i++) {
        tmpinfo = datasetinfo.get(i);
        tmp = new HDFDataSet(tmpinfo.getName(), tmpinfo.getSize(), tmpinfo.getDatatype());
        tmp.setUDM(tmpinfo.getAttributes());
        mH5Dateset.add(tmp);
      }
    }
  }

  private SetAttributeOptions() {
    mPinned = null;
    mTtl = null;
    mTtlAction = TtlAction.DELETE;
    mPersisted = null;
    mOwner = null;
    mGroup = null;
    mMode = Constants.INVALID_MODE;
    mRecursive = false;
    mOperationTimeMs = System.currentTimeMillis();
    mShouldindex = false;
    mBlockIndexBid = new ArrayList<Long>();
    mBlockIndexMax = new ArrayList<Double>();
    mBlockIndexMin = new ArrayList<Double>();
    mBlockIndexVar = new ArrayList<String>();
    mBlockAugIndex = new ArrayList<Long>();
    mUDM = false;
    mPath = new ArrayList<String>();
    mUKey = new ArrayList<String>();
    mUValue = new ArrayList<String>();
    mDeleteAttribute = false;
  }

  /**
   * @return the H5 Dataset
   */
  public ArrayList<HDFDataSet> getH5Dateset() {
    return mH5Dateset;
  }

  /**
   * Set H5 Dataset info.
   * @param value the input dataset info
   */
  public void setH5Dateset(ArrayList<HDFDataSet> value) {
    mHasH5Dataset = true;
    mH5Dateset = value;
  }

  /**
   * @return the user-defined metadata path
   */
  public List<String> getUDMPath() {
    return mPath;
  }

  /**
   * @return the user-defined metadata key
   */
  public List<String> getUDMKey() {
    return mUKey;
  }

  /**
   * @return the user-defined metadata value
   */
  public List<String> getUDMValue() {
    return mUValue;
  }

  /**
   * Set UDM.
   * @param value the input UDM
   */
  public void setUDM(HashMap<String, String> value) {
    mUDM = true;
    String[] tmpkey = value.keySet().toArray(new String[value.size()]);
    String tmpvalue;
    for (int i = 0; i < value.size(); i++) {
      mUKey.add(tmpkey[i]);
      tmpvalue = value.get(tmpkey[i]);
      mUValue.add(tmpvalue);
    }
  }

  /**
   * Set BlockIndex.
   * @param id the block id
   * @param max the max value of current block
   * @param min the min value of current block
   * @param var the var name of current block
   */
  public void setIndexInfo(List<Long> id, List<Double> max, List<Double> min, List<String> var) {
    mShouldindex = true;
    mBlockIndexBid = id;
    mBlockIndexMax = max;
    mBlockIndexMin = min;
    mBlockIndexVar = var;
  }

  /**
   * Set AugIndex.
   * @param augindex the augmented index info
   */
  public void setAugIndex(List<Long> augindex) {
    mBlockAugIndex = augindex;
  }

  /**
   * @return the block id
   */
  public List<Long> getBlockId() {
    return mBlockIndexBid;
  }

  /**
   * @return the block max value list
   */
  public List<Double> getBlockMax() {
    return mBlockIndexMax;
  }

  /**
   * @return the block min value list
   */
  public List<Double> getBlockMin() {
    return mBlockIndexMin;
  }

  /**
   * @return the block var name list
   */
  public List<String> getBlockVar() {
    return mBlockIndexVar;
  }

  /**
   * @return the augmented index info
   */
  public List<Long> getAugIndex() {
    return mBlockAugIndex;
  }

  /**
   * @return the pinned flag value
   */
  public Boolean getPinned() {
    return mPinned;
  }

  /**
   * @return the time-to-live (in seconds)
   */
  public Long getTtl() {
    return mTtl;
  }

  /**
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * @return the persisted flag value
   */
  public Boolean getPersisted() {
    return mPersisted;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the mode bits
   */
  public Short getMode() {
    return mMode;
  }

  /**
   * @return the recursive flag value
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the operation time (in milliseconds)
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the updated options object
   */
  public SetAttributeOptions setPinned(boolean pinned) {
    mPinned = pinned;
    return this;
  }

  /**
   * @param ttl the time-to-live (in seconds) to use
   * @return the updated options object
   */
  public SetAttributeOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public SetAttributeOptions setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return this;
  }

  /**
   * @param persisted the persisted flag value to use
   * @return the updated options object
   */
  public SetAttributeOptions setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param owner the owner to use
   * @return the updated options object
   * @throws IllegalArgumentException if the owner is set to empty
   */
  public SetAttributeOptions setOwner(String owner) {
    if (owner != null && owner.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set owner to empty.");
    }
    mOwner = owner;
    return this;
  }

  /**
   * @param group the group to use
   * @return the updated options object
   * @throws IllegalArgumentException if the group is set to empty
   */
  public SetAttributeOptions setGroup(String group) {
    if (group != null && group.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set group to empty");
    }
    mGroup = group;
    return this;
  }

  /**
   * @param mode the mode bits to use
   * @return the updated options object
   */
  public SetAttributeOptions setMode(short mode) {
    mMode = mode;
    return this;
  }

  /**
   * @param recursive whether owner / group / mode should be updated recursively
   * @return the updated options object
   */
  public SetAttributeOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public SetAttributeOptions setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SetAttributeOptions)) {
      return false;
    }
    SetAttributeOptions that = (SetAttributeOptions) o;
    return Objects.equal(mPinned, that.mPinned)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction)
        && Objects.equal(mPersisted, that.mPersisted)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mRecursive, that.mRecursive)
        && mOperationTimeMs == that.mOperationTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPinned, mTtl, mTtlAction, mPersisted, mOwner, mGroup, mMode,
        mRecursive, mOperationTimeMs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("pinned", mPinned)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction)
        .add("persisted", mPersisted)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .add("recursive", mRecursive)
        .add("operationTimeMs", mOperationTimeMs)
        .toString();
  }
}
