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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;
import alluxio.security.authorization.Mode;
import alluxio.thrift.SetAttributeTOptions;
import alluxio.thrift.H5DatasetInfo;
import alluxio.wire.HDFDataSet;
import alluxio.wire.ThriftUtils;
import alluxio.wire.TtlAction;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import java.util.List;
import java.util.ArrayList;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for setting any number of a path's attributes. If a value is set as null, it
 * will be interpreted as an unset value and the current value will be unchanged.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class SetAttributeOptions {
  private Boolean mPinned;
  private Long mTtl;
  private TtlAction mTtlAction;
  // SetAttribute for persist will be deprecated in Alluxio 1.4 release.
  private Boolean mPersisted;
  private String mOwner;
  private String mGroup;
  private Mode mMode;
  private boolean mRecursive;
  private boolean mShouldindex;
  private boolean mUDM;
  private List<Long> mBlockIndexBid;
  private List<Double> mBlockIndexMax;
  private List<Double> mBlockIndexMin;
  private List<String> mBlockIndexVar;
  private List<Long> mBlockAugIndex;
  private List<String> mPath;
  private List<String> mUKey;
  private List<String> mUValue;
  private boolean mDeleteAttribute;
  private boolean mHasH5Dataset;
  private List<HDFDataSet> mH5Dateset;

  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new SetAttributeOptions();
  }

  private SetAttributeOptions() {
    mPinned = null;
    mTtl = null;
    mTtlAction = TtlAction.DELETE;
    mPersisted = null;
    mOwner = null;
    mGroup = null;
    mMode = null;
    mRecursive = false;
    mBlockIndexBid = new ArrayList<Long>();
    mBlockIndexMax = new ArrayList<Double>();
    mBlockIndexMin = new ArrayList<Double>();
    mBlockIndexVar = new ArrayList<String>();
    mBlockAugIndex = new ArrayList<Long>();
    mPath = new ArrayList<String>();
    mUKey = new ArrayList<String>();
    mUValue = new ArrayList<String>();
    mShouldindex = false;
    mUDM = false;
    mDeleteAttribute = false;
    mHasH5Dataset = false;
    mH5Dateset  = new ArrayList<HDFDataSet>();
  }

  /**
   * Add H5 Dataset info.
   * @param value the input dataset info
   */
  public void addH5(List<HDFDataSet> value) {
    mHasH5Dataset = true;
    mH5Dateset = value;
  }

  /**
   * Add User-defined metadata.
   * @param key the key of User-defined metadata
   * @param value the value of User-defined metadata
   */
  public void addUDM(String key, String value) {
    mUDM = true;
    mUKey.add(key);
    mUValue.add(value);
  }

  /**
   * Delete User-defined metadata.
   * @param key the key of User-defined metadata to delete
   * @param value the value of User-defined metadata to delete
   */
  public void deleteDUM(String key, String value) {
    mUDM = true;
    mDeleteAttribute = true;
    mUKey.add(key);
    mUValue.add(value);
  }

  /**
   * Add User-defined metadata to target path.
   * @param path the file path
   */
  public void addUDMPath(String path) {
    mUDM = true;
    mPath.add(path);
  }

  /**
   * Delete User-defined metadata to target path.
   * @param path the target file path
   */
  public void deleteUDMPath(String path) {
    mPath.add(path);
  }

  /**
   * Add BlockIndex info.
   * @param bid the block id
   * @param max the max value of current block
   * @param min the min value of current block
   * @param var the var name of current block
   * @param tmpbitmap the augmented bitmap index
   */
  public void addBlockIndex(long bid, double max, double min, String var, long tmpbitmap) {
    mShouldindex = true;
    mBlockIndexBid.add(bid);
    mBlockIndexMax.add(max);
    mBlockIndexMin.add(min);
    mBlockIndexVar.add(var);
    mBlockAugIndex.add(tmpbitmap);
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
   * @return the pinned flag value; it specifies whether the object should be kept in memory
   */
  public Boolean getPinned() {
    return mPinned;
  }

  /**
   * @return the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *         created file should be kept around before it is automatically deleted, irrespective of
   *         whether the file is pinned
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
   * @deprecated the persisted attribute is deprecated since version 1.4 and will be removed in 2.0
   * @return the persisted value of the file; it denotes whether the file has been persisted to the
   *         under file system or not.
   */
  @Deprecated
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
   * @return the mode
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return the recursive flag value
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @param pinned the pinned flag value to use; it specifies whether the object should be kept in
   *        memory, if ttl(time to live) is set, the file will be deleted after expiration no
   *        matter this value is true or false
   * @return the updated options object
   */
  public SetAttributeOptions setPinned(boolean pinned) {
    mPinned = pinned;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, irrespective of
   *        whether the file is pinned
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
   * @deprecated the persisted attribute is deprecated since version 1.4 and will be removed in 2.0
   * @param persisted the persisted flag value to use; it specifies whether the file has been
   *        persisted in the under file system or not.
   * @return the updated options object
   */
  @Deprecated
  public SetAttributeOptions setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
  }

  /**
   * @param owner to be set as the owner of a path
   * @return the updated options object
   * @throws IllegalArgumentException if the owner is set to empty
   */
  public SetAttributeOptions setOwner(String owner) throws IllegalArgumentException {
    if (owner != null && owner.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set owner to empty.");
    }
    mOwner = owner;
    return this;
  }

  /**
   * @param group to be set as the group of a path
   * @return the updated options object
   * @throws IllegalArgumentException if the group is set to empty
   */
  public SetAttributeOptions setGroup(String group) throws IllegalArgumentException {
    if (group != null && group.isEmpty()) {
      throw new IllegalArgumentException("It is not allowed to set group to empty");
    }
    mGroup = group;
    return this;
  }

  /**
   * @param mode to be set as the mode of a path
   * @return the updated options object
   */
  public SetAttributeOptions setMode(Mode mode) {
    mMode = mode;
    return this;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive whether to set acl recursively under a directory
   * @return the updated options object
   */
  public SetAttributeOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @return Thrift representation of the options
   */
  public SetAttributeTOptions toThrift() {
    SetAttributeTOptions options = new SetAttributeTOptions();
    if (mPinned != null) {
      options.setPinned(mPinned);
    }
    if (mTtl != null) {
      options.setTtl(mTtl);
      options.setTtlAction(ThriftUtils.toThrift(mTtlAction));
    }

    if (mHasH5Dataset) {
      List<H5DatasetInfo> datasetList = new ArrayList<>();
      H5DatasetInfo tmpinfo;
      for (int i = 0; i < mH5Dateset.size(); i++) {
        tmpinfo = mH5Dateset.get(i).toThrift();
        datasetList.add(tmpinfo);
      }
      options.setDatasetIsSet(true);
      options.setDataset(datasetList);
    }

    if (mShouldindex) {
      options.setMIndexInfo_BlockId(mBlockIndexBid);
      options.setMIndexInfo_MaxValue(mBlockIndexMax);
      options.setMIndexInfo_MinValue(mBlockIndexMin);
      options.setMIndexInfo_VarName(mBlockIndexVar);
      options.setMAugIndex(mBlockAugIndex);
    }

    if (mUDM) {
      if (!mDeleteAttribute) {
        options.setMPath(mPath);
        options.setMUKey(mUKey);
        options.setMUValue(mUValue);
      } else {
        options.setMDeleteAttribute(true);
        options.setMPath(mPath);
        options.setMUKey(mUKey);
        options.setMUValue(mUValue);
      }
    }

    if (mPersisted != null) {
      options.setPersisted(mPersisted);
    }
    if (mOwner != null) {
      options.setOwner(mOwner);
    }
    if (mGroup != null) {
      options.setGroup(mGroup);
    }
    if (mMode != null) {
      options.setMode(mMode.toShort());
    }
    options.setRecursive(mRecursive);
    return options;
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
        && Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPinned, mTtl, mTtlAction, mPersisted, mOwner,
        mGroup, mMode, mRecursive);
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
        .toString();
  }
}
