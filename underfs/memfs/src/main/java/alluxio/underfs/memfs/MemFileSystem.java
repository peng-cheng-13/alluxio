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

/**
 *  The Java API of MemFileSystem.
 */
public class MemFileSystem {

  static {
    System.loadLibrary("jninrfs");
  }

  /**
   * Connect to the mem file system.
   * @return 0 if succeed
   */
  public native int nrfsConnect();

  /**
   * DisConnect to the mem file system.
   * @return 0 if succeed
   */
  public native int nrfsDisconnect();

  /**
   * Open a file.
   * @param buf the file path
   * @param flags the open flags
   * @return 0 if succeed
   */
  public native long nrfsOpenFile(byte[] buf, int flags);

  /**
   * Close a file.
   * @param buf the file path
   * @return 0 if succeed
   */
  public native int nrfsCloseFile(byte[] buf);

  /**
   * Create a file.
   * @param buf the file path
   * @return 0 if succeed
   */
  public native int nrfsMknod(byte[] buf);

  /**
   * Check whether a file exists.
   * @param buf the file path
   * @return 0 if succeed
   */
  public native int nrfsAccess(byte[] buf);

  /**
   * Get the attribute of the file.
   * @param buf the file path
   * @param properties the file properties
   * @return 0 if succeed
   */
  public native int nrfsGetAttibute(byte[] buf, int[] properties);

  /**
   * Write data to a file.
   * @param path the file path
   * @param buffer the write data
   * @param size size of write data
   * @param offset offset of the write operation
   * @return 0 if succeed
   */
  public native int nrfsWrite(byte[] path, byte[] buffer, long size, long offset);

  /**
   * Read data from a file.
   * @param path the file path
   * @param buffer the write data
   * @param size size of write data
   * @param offset offset of the write operation
   * @return 0 if succeed
   */
  public native int nrfsRead(byte[] path, byte[] buffer, long size, long offset);

  /**
   * Create a directory.
   * @param path the dir path
   * @return 0 if succeed
   */
  public native int nrfsCreateDirectory(byte[] path);

  /**
   * Delete a file/dir.
   * @param path the file/dir path
   * @return 0 if succeed
   */
  public native int nrfsDelete(byte[] path);

  /**
   * Rename a file/dir.
   * @param oldpath the old path
   * @param newpath the new path
   * @return 0 if succeed
   */
  public native int nrfsRename(byte[] oldpath, byte[] newpath);

  /**
   * List a directory.
   * @param path the dir path
   * @return the file path array
   */
  public native String[] nrfsListDirectory(byte[] path);

}
