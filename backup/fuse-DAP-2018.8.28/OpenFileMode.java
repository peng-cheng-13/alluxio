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

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Convenience class to encapsulate input/output streams of open alluxio files.
 *
 * An open file can be either write-only or read-only, never both. This means that one of getIn or
 * getOut will be null, while the other will be non-null. It is up to the user of this class
 * (currently, only {@link AlluxioFuseFileSystem}) to check that.
 *
 * This mechanism is preferred over more complex sub-classing to avoid useless casts or type checks
 * for every read/write call, which happen quite often.
 */
@NotThreadSafe
final class OpenFileMode implements Closeable {
  private String mDAP;
  private String mTier;

  public OpenFileMode(String DAP, String Tier) {
    mDAP = DAP;
    mTier = Tier;
  }

  public void addDAP(String DAP) {
    mDAP = DAP;
  }

  public void addTier(String Tier) {
    mTier = Tier;
  }

  public String getDAP() {
    return mDAP;
  }

  public String getTier() {
    return mTier;
  }
  /**
   * Closes the underlying open streams.
   */
  @Override
  public void close() throws IOException {
    if (mDAP != null) {
      mDAP = null;
    }

    if (mTier != null) {
      mTier = null;
    }
  }
}
