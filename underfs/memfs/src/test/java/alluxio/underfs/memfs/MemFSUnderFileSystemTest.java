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
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Unit tests for the {@link MemFSUnderFileSystemTest}.
 */
public class MemFSUnderFileSystemTest {
  private String mMemUfsRoot;
  private MemFSUnderFileSystem mMemUfs;

  @Before
  public void before() throws IOException {
    mMemUfsRoot = "/MFS";
    UnderFileSystemConfiguration conf = UnderFileSystemConfiguration.defaults();
    mMemUfs = new MemFSUnderFileSystem(new AlluxioURI(mMemUfsRoot), conf);
  }

  @Test
  public void exists() throws IOException {
    String filepath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    CreateOptions coption = CreateOptions.defaults();
    mMemUfs.create(filepath, coption).close();

    Assert.assertTrue(mMemUfs.isFile(filepath));

    mMemUfs.deleteFile(filepath);
  }

  @Test
  public void create() throws IOException {
    String filepath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    CreateOptions coption = CreateOptions.defaults();
    OutputStream os = mMemUfs.create(filepath, coption);
    os.close();

    Assert.assertTrue(mMemUfs.isFile(filepath));
    mMemUfs.deleteFile(filepath);
  }

  @Test
  public void deleteFile() throws IOException {
    String filepath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    CreateOptions coption = CreateOptions.defaults();
    mMemUfs.create(filepath, coption).close();
    mMemUfs.deleteFile(filepath);

    Assert.assertFalse(mMemUfs.isFile(filepath));
  }

  @Test
  public void recursiveDelete() throws IOException {
    String dirpath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    MkdirsOptions options = MkdirsOptions.defaults();
    mMemUfs.mkdirs(dirpath, options);
    String filepath = PathUtils.concatPath(dirpath, getUniqueFileName());
    CreateOptions coption = CreateOptions.defaults();
    mMemUfs.create(filepath, coption).close();
    mMemUfs.deleteDirectory(dirpath, DeleteOptions.defaults().setRecursive(true));

    Assert.assertFalse(mMemUfs.isDirectory(dirpath));
  }

  @Test
  public void nonRecursiveDelete() throws IOException {
    String dirpath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    MkdirsOptions options = MkdirsOptions.defaults();
    mMemUfs.mkdirs(dirpath, options);
    String filepath = PathUtils.concatPath(dirpath, getUniqueFileName());
    CreateOptions coption = CreateOptions.defaults();
    mMemUfs.create(filepath, coption).close();
    mMemUfs.deleteDirectory(dirpath, DeleteOptions.defaults().setRecursive(false));

    Assert.assertTrue(mMemUfs.isDirectory(dirpath));
  }

  @Test
  public void mkdirs() throws IOException {
    String parentPath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    String dirpath = PathUtils.concatPath(parentPath, getUniqueFileName());
    mMemUfs.mkdirs(dirpath, MkdirsOptions.defaults());

    Assert.assertTrue(mMemUfs.isDirectory(dirpath));

    File file = new File(dirpath);
    Assert.assertTrue(file.exists());
  }

  @Test
  public void mkdirsWithCreateParentEqualToFalse() throws IOException {
    String parentPath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    String dirpath = PathUtils.concatPath(parentPath, getUniqueFileName());
    mMemUfs.mkdirs(dirpath, MkdirsOptions.defaults().setCreateParent(false));

    Assert.assertFalse(mMemUfs.isDirectory(dirpath));
  }

  @Test
  public void open() throws IOException {
    byte[] bytes = getBytes();
    String filepath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());

    OutputStream os = mMemUfs.create(filepath, CreateOptions.defaults());
    os.write(bytes);
    os.close();

    InputStream is = mMemUfs.open(filepath, OpenOptions.defaults());
    byte[] bytes1 = new byte[bytes.length];
    is.read(bytes1);
    is.close();

    Assert.assertArrayEquals(bytes, bytes1);
  }

  @Test
  public void getFileLocations() throws IOException {
    byte[] bytes = getBytes();
    String filepath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());

    OutputStream os = mMemUfs.create(filepath);
    os.write(bytes);
    os.close();

    List<String> fileLocations = mMemUfs.getFileLocations(filepath);
    Assert.assertEquals(1, fileLocations.size());
    Assert.assertEquals(NetworkAddressUtils.getLocalHostName(), fileLocations.get(0));
  }

  @Test
  public void isFile() throws IOException {
    String dirpath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    mMemUfs.mkdirs(dirpath, MkdirsOptions.defaults());
    Assert.assertFalse(mMemUfs.isFile(dirpath));

    String filepath = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    mMemUfs.create(filepath, CreateOptions.defaults()).close();
    Assert.assertTrue(mMemUfs.isFile(filepath));
  }

  @Test
  public void renameFile() throws IOException {
    byte[] bytes = getBytes();
    String filepath1 = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());

    OutputStream os = mMemUfs.create(filepath1, CreateOptions.defaults());
    os.write(bytes);
    os.close();

    String filepath2 = PathUtils.concatPath(mMemUfsRoot, getUniqueFileName());
    mMemUfs.renameFile(filepath1, filepath2);

    InputStream is = mMemUfs.open(filepath2, OpenOptions.defaults());
    byte[] bytes1 = new byte[bytes.length];
    is.read(bytes1);
    is.close();

    Assert.assertArrayEquals(bytes, bytes1);
  }

  private byte[] getBytes() {
    String s = "BYTES";
    return s.getBytes();
  }

  private String getUniqueFileName() {
    long time = System.nanoTime();
    String fileName = "" + time;
    return fileName;
  }
}
