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

package alluxio.client.file;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Method options for creating a file.
 */
public final class UserDefinedPatterns implements Serializable {
  private static final long serialVersionUID = 1111066306933531779L;
  private int mWriteTier;
  private long mBlockSizeBytes;
  private String mLocationPolicy;
  private String mLoadBalanceStrategy;
  private String mHost;

  /**
   * @return the default {@link UserDefinedPatterns}
   */
  public static UserDefinedPatterns defaults() {
    return new UserDefinedPatterns();
  }

  private UserDefinedPatterns() {
    mWriteTier = 0;
    mBlockSizeBytes = 512 * 1024 * 1024;
    mLocationPolicy = "RoundRobinPolicy";
    mLoadBalanceStrategy = "MaxFree";
    mHost = null;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the location policy class used when storing data to Alluxio
   */
  public String getLocationPolicyClass() {
    return mLocationPolicy;
  }

  /**
   * @return the write tier
   */
  public int getWriteTier() {
    return mWriteTier;
  }

  /**
   * @return the type of load balance strategy
   */
  public String getLoadBalanceStrategy() {
    return mLoadBalanceStrategy;
  }

  /**
   * @return the target host
   */
  public String getHost() {
    return mHost;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated options object
   */
  public UserDefinedPatterns setBlockSizeBytes(long blockSizeBytes) {
    mBlockSizeBytes = blockSizeBytes;
    return this;
  }

  /**
   * @param className the location policy class to use when storing data to TDMS
   * @return the updated options object
   */
  public UserDefinedPatterns setLocationPolicy(String className) {
    mLocationPolicy = className;
    return this;
  }

  /**
   * @param className the load balance strategy to use when storing data to TDMS
   * @return the updated options object
   */
  public UserDefinedPatterns setLoadBalanceStrategy(String className) {
    mLoadBalanceStrategy = className;
    return this;
  }

  /**
   * @param writeTier the write tier to use for this operation
   * @return the updated options object
   */
  public UserDefinedPatterns setWriteTier(int writeTier) {
    mWriteTier = writeTier;
    return this;
  }

  /**
   * @param host the specified worker that hold the data
   * @return the updated options object
   */
  public UserDefinedPatterns setHost(String host) {
    mHost = host;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("locationPolicy", mLocationPolicy)
        .add("writeTier", mWriteTier)
        .toString();
  }
}
