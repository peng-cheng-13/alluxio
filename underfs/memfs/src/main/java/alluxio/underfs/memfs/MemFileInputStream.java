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

package alluxio.underfs.memfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;

/**
 *  The Java API of MemFileSystem.
 */
@NotThreadSafe
public final class MemFileInputStream extends InputStream {

  private final byte[] mFilePath;
  private final String mPath;
  private final MemFileSystem mFileSystem;
  private long mPos;
  private static final Logger LOG = LoggerFactory.getLogger(MemFileInputStream.class);

  /**
   * Constructs a new {@link MemFileInputStream}.
   * @param fs the Memfilesystem
   * @param path the write file path
   */
  public MemFileInputStream(MemFileSystem fs, String path) {
    this(fs, path, 0L);
  }

  /**
   * Constructs a new {@link MemFileInputStream}.
   * @param fs the Memfilesystem
   * @param path the write file path
   * @param pos the position to start
   */
  public MemFileInputStream(MemFileSystem fs, String path, long pos) {
    mFileSystem = fs;
    mPath = path;
    mFilePath = path.getBytes();
    mPos = pos;
    int ret = (int) mFileSystem.nrfsOpenFile(mFilePath, 0);
    if (ret != 1) {
      LOG.warn("File open failed");
      System.exit(0);
    }
  }

  @Override
  public int available() throws IOException {
    return 100;
  }

  @Override
  public void mark(int readlimit) {
    LOG.info("mark readlimit " + readlimit);
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  /**
   * Implement the read opertaion.
   * @return int value
   */
  public int read() throws IOException {
    LOG.info("Read from " + mPath + " once, size is 4");
    byte[] readdata = new byte[1];
    int value;
    int ret = mFileSystem.nrfsRead(mPath.getBytes(), readdata, 1, mPos);
    LOG.info("Read succeed");
    if (ret != -1) {
      value = (int) readdata[0];
      mPos++;
      return value;
    }
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    LOG.info("Read from " + mPath + " once, size is " + b.length + ", offset is " + mPos);
    long len = (long) b.length;
    int ret = mFileSystem.nrfsRead(mPath.getBytes(), b, len, mPos);
    if (ret != -1) {
      mPos += b.length;
      LOG.info("Read succeed");
    }
    return ret;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    LOG.info("Read from " + mPath + " once, size is " + b.length + ", offset is " + mPos);
    int ret = mFileSystem.nrfsRead(mPath.getBytes(), b, (long) len, (long) off);
    if (ret != -1) {
      mPos += ret;
      LOG.info("Read succeed");
    }
    return ret;
  }

  @Override
  public void reset() throws IOException {
    LOG.info("Reset once");
    mPos = 0;
  }

  @Override
  public long skip(long n) throws IOException {
    LOG.info("Skip offset " + n);
    mPos += n;
    return mPos;
  }

  @Override
  public void close() throws IOException {
    mFileSystem.nrfsCloseFile(mFilePath);
  }

  /*
   * Bute[] to int.
   * @param input input byte[]
   * @return int
   */
  private int Byte2Int(byte[] input) {
    int num;
    num = ((input[0] & 0xff) << 24) | ((input[1] & 0x00) << 16) | ((input[2] & 0x00) << 8)
        | (input[3] & 0x00);
    return num;
  }

}
