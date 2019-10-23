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
import java.io.OutputStream;

/**
 *  The Java API of MemFileSystem.
 */
@NotThreadSafe
public class MemFileOutputStream extends OutputStream {

  private byte[] mFilePath;
  private String mPath;
  private MemFileSystem mFileSystem;
  private long mOffset;
  private static final Logger LOG = LoggerFactory.getLogger(MemFileOutputStream.class);

  /**
   * Constructs a new {@link MemFileOutputStream}.
   * @param path the write file path
   * @param fs the MemFileSystem
   */
  public MemFileOutputStream(MemFileSystem fs, String path) {
    int ret;
    mFileSystem = fs;
    mFilePath = path.getBytes();
    mPath = path;
    mOffset = 0L;
    ret = (int) mFileSystem.nrfsOpenFile(mFilePath, 1);
    LOG.info("Open File with ret: " + ret);
    if (ret != 1) {
      System.out.println("File creation failed");
      System.exit(0);
    }
  }

  @Override
  public void write(int b) throws IOException {
    LOG.info("Write int");
    int ret = mFileSystem.nrfsWrite(mFilePath, IntToByte(b), 4, mOffset);
    mOffset += 4;
  }

  @Override
  public void write(byte[] b) throws IOException {
    LOG.info("Write to " + mPath + " once, size is " + b.length + ", offset is " + mOffset);
    long len = (long) b.length;
    int ret = mFileSystem.nrfsWrite(mPath.getBytes(), b, len, mOffset);
    if (ret != -1) {
      LOG.info("Write succeed");
      mOffset += len;
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    LOG.info("Write to " + mPath + " once, size is " + b.length + ", offset is " + off);
    int ret = (int) mFileSystem.nrfsWrite(mPath.getBytes(), b, (long) len, (long) off);
    if (ret != -1) {
      LOG.info("Write succeed");
      mOffset = off + len;
    }
  }

  @Override
  public void flush() throws IOException {

  }

  @Override
  public void close() throws IOException {
    mFileSystem.nrfsCloseFile(mFilePath);
  }

  /**
   * Int to byte[].
   * @param num input
   * @return output byte array
   */
  private byte[] IntToByte(int num) {
    byte[] output = new byte[4];
    output[0] = (byte) ((num >> 24) & 0xff);
    output[1] = (byte) ((num >> 16) & 0xff);
    output[2] = (byte) ((num >> 8) & 0xff);
    output[3] = (byte) (num & 0xff);
    return output;
  }

}
