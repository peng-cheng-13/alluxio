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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.underfs.AtomicFileOutputStream;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.BaseUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Local FS {@link UnderFileSystem} implementation.
 * <p>
 * This is primarily intended for local unit testing and single machine mode. In principle, it can
 * also be used on a system where a shared file system (e.g. NFS) is mounted at the same path on
 * every node of the system. However, it is generally preferable to use a proper distributed file
 * system for that scenario.
 * </p>
 */
@ThreadSafe
public class MemFSUnderFileSystem extends BaseUnderFileSystem
    implements AtomicFileOutputStreamCallback {
  private static final Logger LOG = LoggerFactory.getLogger(MemFSUnderFileSystem.class);
  private MemFileSystem mFileSystem;

  /**
   * Constructs a new {@link MemFSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ufsConf UFS configuration
   */
  public MemFSUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
    mFileSystem = new MemFileSystem();
    int ret = mFileSystem.nrfsConnect();
    if (ret != 0) {
      LOG.warn("Connet to memory file system failed");
      System.exit(0);
    }
  }

  @Override
  public String getUnderFSType() {
    return "memfs";
  }

  @Override
  public void close() throws IOException {
    mFileSystem.nrfsDisconnect();
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    if (!options.isEnsureAtomic()) {
      return createDirect(path, options);
    }
    return new AtomicFileOutputStream(path, this, options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    LOG.info("MemFSUnderFileSystem:: createDirect");
    path = stripPath(path);
    OutputStream os = new MemFileOutputStream(mFileSystem, path);
    LOG.info("MemFSUnderFileSystem:: create succeed");
    return os;
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    LOG.info("MemFSUnderFileSystem:: deleteDirectory");
    path = stripPath(path);
    int ret = mFileSystem.nrfsAccess(path.getBytes());
    if (ret == 1) {
      LOG.warn("Path " + path + " is a file.");
      return false;
    } else if (ret == 0) {
      boolean success = true;
      if (options.isRecursive()) {
        String[] files = mFileSystem.nrfsListDirectory(path.getBytes());
        if (files != null) {
          for (String child : files) {
            String childPath = PathUtils.concatPath(path, child);
            // If child is directory
            if (mFileSystem.nrfsAccess(childPath.getBytes()) == 0) {
              success = success && deleteDirectory(childPath,
                  DeleteOptions.defaults().setRecursive(true));
            } else {
              success = success && deleteFile(PathUtils.concatPath(path, child));
            }
          }
        }
      }
      return success;
    } else {
      LOG.warn("Path " + path + " does not exist.");
    }
    return false;

  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: deleteFile");
    path = stripPath(path);
    int ret = mFileSystem.nrfsDelete(path.getBytes());
    if (ret == 0) {
      return true;
    }
    return false;
  }

  @Override
  public boolean exists(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: exists");
    path = stripPath(path);
    int ret = mFileSystem.nrfsAccess(path.getBytes());
    if (ret != -1) {
      return true;
    }
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    //path = stripPath(path);
    if (!exists(path)) {
      throw new FileNotFoundException(path);
    }
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: getDirectoryStatus");
    String tpath = stripPath(path);
    int ret = mFileSystem.nrfsAccess(tpath.getBytes());
    if (ret != 0) {
      LOG.warn("Path " + path + " does not exist or is a file");
    }
    return new UfsDirectoryStatus(path, "MemFS", "MemFS", (short) 777);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: getFileLocations");
    path = stripPath(path);
    List<String> ret = new ArrayList<>();
    ret.add(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC));
    return ret;
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return getFileLocations(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: getFileStatus");
    String tpath = stripPath(path);
    int ret = mFileSystem.nrfsAccess(tpath.getBytes());
    if (ret != 1) {
      LOG.warn("Path " + path + " does not exist or is a directory");
    }
    int[] fileAttr = new int[2];
    mFileSystem.nrfsGetAttibute(tpath.getBytes(), fileAttr);
    return new UfsFileStatus(path, (long) fileAttr[0], 0L, "MemFS",
        "MemFS", (short) 777);
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    LOG.info("MemFSUnderFileSystem:: getSpace");
    path = stripPath(path);
    switch (type) {
      case SPACE_TOTAL:
        return 10000L;
      case SPACE_FREE:
        return 0L;
      case SPACE_USED:
        return 0L;
      default:
        throw new IOException("Unknown space type: " + type);
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: isDirectory");
    path = stripPath(path);
    int ret = mFileSystem.nrfsAccess(path.getBytes());
    if (ret == 0) {
      return true;
    }
    return false;
  }

  @Override
  public boolean isFile(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: isFile");
    path = stripPath(path);
    int ret = mFileSystem.nrfsAccess(path.getBytes());
    if (ret == 1) {
      return true;
    }
    return false;
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    LOG.info("MemFSUnderFileSystem:: listStatus");
    String tpath = stripPath(path);
    int ret = mFileSystem.nrfsAccess(tpath.getBytes());
    if (ret != 0) {
      LOG.warn("Path " + tpath + " does not exist or is a file");
      return null;
    }
    String[] files = mFileSystem.nrfsListDirectory(tpath.getBytes());
    LOG.info("Debug listStatus:: nrfsListDirectory return " + files.length + " childrens");
    UfsStatus[] rtn = new UfsStatus[files.length];
    int i = 0;
    for (String file : files) {
      String childPath = PathUtils.concatPath(tpath, file);
      String tchildPath = path.concat("/").concat(file);
      LOG.info("Parent is " + path + ", child is " + file
          + ", Current child is " + childPath + ", tchildPath is " + tchildPath);
      UfsStatus retStatus;
      ret = mFileSystem.nrfsAccess(childPath.getBytes());
      if (ret == 0) {
        retStatus = new UfsDirectoryStatus(file, "owner-memfs",
              "group-memfs", (short) 777);
      } else if (ret == 1) {
        int[] fileAttr = new int[2];
        mFileSystem.nrfsGetAttibute(childPath.getBytes(), fileAttr);
        retStatus = new UfsFileStatus(file,
            fileAttr[0], 0L, "owner-memfs", "group-memfs", (short) 777);
      } else {
        LOG.warn("Path " + childPath + " does not exist or is a file");
        break;
      }
      rtn[i++] = retStatus;
    }
    return rtn;
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    LOG.info("MemFSUnderFileSystem:: mkdirs");
    path = stripPath(path);

    if (!options.getCreateParent()) {
      if (mFileSystem.nrfsCreateDirectory(path.getBytes()) == 0) {
        setMode(path, options.getMode().toShort());
        FileUtils.setLocalDirStickyBit(path);
        try {
          setOwner(path, options.getOwner(), options.getGroup());
        } catch (IOException e) {
          LOG.warn("Failed to update the ufs dir ownership, default values will be used: {}",
              e.getMessage());
        }
        return true;
      }
      return false;
    } else {
      LOG.warn("Create one directory each time");
      if (mFileSystem.nrfsCreateDirectory(path.getBytes()) != 0) {
        LOG.warn("Create directory faled");
      }
    }
    return true;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    LOG.info("MemFSUnderFileSystem:: open");
    path = stripPath(path);
    MemFileInputStream inputStream = new MemFileInputStream(mFileSystem, path, options.getOffset());
    LOG.info("MemFSUnderFileSystem:: open succeed");
    return inputStream;
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    if (!isDirectory(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a file.", src, dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    if (!isFile(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a directory.", src,
          dst);
      return false;
    }
    return rename(src, dst);
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    LOG.info("MemFSUnderFileSystem:: setOwner");
    path = stripPath(path);
    // No-op
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    LOG.info("MemFSUnderFileSystem:: setMode");
    path = stripPath(path);
    // No-op
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    // No-op
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    // No-op
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  /**
   * Rename a file to a file or a directory to a directory.
   *
   * @param src path of source file or directory
   * @param dst path of destination file or directory
   * @return true if rename succeeds
   */
  private boolean rename(String src, String dst) throws IOException {
    LOG.info("MemFSUnderFileSystem:: rename");
    src = stripPath(src);
    dst = stripPath(dst);
    int ret = mFileSystem.nrfsRename(src.getBytes(), dst.getBytes());
    if (ret == 0) {
      return true;
    }
    return false;
  }

  /**
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
    String translatedPath = new AlluxioURI(path).getPath();
    LOG.warn("stripPath:: Original path is " + path + ", translatedPath is " + translatedPath);
    return translatedPath;
  }
}
