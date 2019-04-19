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

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.annotation.PublicApi;
import alluxio.PropertyKey;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RoundRobinPolicy;
import alluxio.client.file.policy.SpecificHostPolicy;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MountPointInfo;
import alluxio.wire.QueryInfo;
import alluxio.wire.HDFDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import krati.store.SerializableObjectStore;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import javax.annotation.concurrent.ThreadSafe;

/**
* Default implementation of the {@link FileSystem} interface. Developers can extend this class
* instead of implementing the interface. This implementation reads and writes data through
* {@link FileInStream} and {@link FileOutStream}. This class is thread safe.
*/
@PublicApi
@ThreadSafe
public class BaseFileSystem implements FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFileSystem.class);

  protected final FileSystemContext mFileSystemContext;

  //private static PathStore sPathStore;
  private static SerializableObjectStore<String, Set<String>> sPathStore;
  private static HashStore sValueStore;
  private static List<HDFDataSet> sDatasetInfo;
  private static PatternStore sPatternStore;
  private static CreateFileOptions sCreateFileOption;
  private static int sNum = 0;

  /**
   * @param context file system context
   * @return a {@link BaseFileSystem}
   */
  public static BaseFileSystem get(FileSystemContext context) {
    try {
      String storepath = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
      sValueStore = new HashStore(new File(storepath.concat("/ValueStore")), 10240);
      sPatternStore = new PatternStore(new File(storepath.concat("/PatternStore")), 1024);
      //sPathStore = new HashStore(new File(storepath.concat("/PathStore")), 10240);
      sPathStore = PathStore.getDataStore();
      sDatasetInfo = new ArrayList<>();
      sCreateFileOption = CreateFileOptions.defaults();
    } catch (Exception e) {
      System.out.println("Init HashStore failed");
    }
    return new BaseFileSystem(context);
  }

  /**
   * Constructs a new base file system.
   *
   * @param context file system context
   */
  protected BaseFileSystem(FileSystemContext context) {
    mFileSystemContext = context;
  }

  @Override
  public void createDirectory(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    createDirectory(path, CreateDirectoryOptions.defaults());
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.createDirectory(path, options);
      LOG.debug("Created directory {}, options: {}", path.getPath(), options);
    } catch (AlreadyExistsException e) {
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (InvalidArgumentException e) {
      throw new InvalidPathException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileOutStream createFile(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    return createFile(path, sCreateFileOption);
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFileOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    URIStatus status;
    try {
      masterClient.createFile(path, options);
      status = masterClient.getStatus(path, GetStatusOptions.defaults().setLoadMetadataType(
          LoadMetadataType.Never));
      LOG.debug("Created file {}, options: {}", path.getPath(), options);
    } catch (AlreadyExistsException e) {
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (InvalidArgumentException e) {
      throw new InvalidPathException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
    OutStreamOptions outStreamOptions = options.toOutStreamOptions();
    outStreamOptions.setUfsPath(status.getUfsPath());
    outStreamOptions.setMountId(status.getMountId());
    FileOutStream mout = new FileOutStream(path, outStreamOptions, mFileSystemContext);
    if (options.isIndex()) {
      mout.setFileInfo(options.getVarName(), options.getVarType());
    }
    return mout;
  }

  @Override
  public void defineDax(String path) throws IOException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.defineDax(path);
      LOG.debug("Define DAX {}, ", path);
    } catch (IOException e) {
      throw e;
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void delete(AlluxioURI path)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    delete(path, DeleteOptions.defaults());
  }

  @Override
  public void delete(AlluxioURI path, DeleteOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.delete(path, options);
      LOG.debug("Deleted {}, options: {}", path.getPath(), options);
    } catch (FailedPreconditionException e) {
      // A little sketchy, but this should be the only case that throws FailedPrecondition.
      throw new DirectoryNotEmptyException(e.getMessage());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean exists(AlluxioURI path)
      throws InvalidPathException, IOException, AlluxioException {
    return exists(path, ExistsOptions.defaults());
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this more efficient
      masterClient.getStatus(path, options.toGetStatusOptions());
      return true;
    } catch (NotFoundException e) {
      return false;
    } catch (InvalidArgumentException e) {
      // The server will throw this when a prefix of the path is a file.
      // TODO(andrew): Change the server so that a prefix being a file means the path does not exist
      return false;
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void free(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    free(path, FreeOptions.defaults());
  }

  @Override
  public void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.free(path, options);
      LOG.debug("Freed {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public URIStatus getStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return getStatus(path, GetStatusOptions.defaults());
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      return masterClient.getStatus(path, options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return listStatus(path, ListStatusOptions.defaults());
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    // TODO(calvin): Fix the exception handling in the master
    try {
      return masterClient.listStatus(path, options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    loadMetadata(path, LoadMetadataOptions.defaults());
  }

  /**
   * {@inheritDoc}
   *
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  @Override
  public void loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.loadMetadata(path, options);
      LOG.debug("Loaded metadata {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath)
      throws IOException, AlluxioException {
    mount(alluxioPath, ufsPath, MountOptions.defaults());
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Make this fail on the master side
      masterClient.mount(alluxioPath, ufsPath, options);
      LOG.info("Mount " + ufsPath.toString() + " to " + alluxioPath.getPath());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      return masterClient.getMountTable();
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileInStream openFile(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    return openFile(path, OpenFileOptions.defaults());
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFileOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    URIStatus status = getStatus(path);
    if (status.isFolder()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
    }
    InStreamOptions inStreamOptions = options.toInStreamOptions();
    return FileInStream.create(status, inStreamOptions, mFileSystemContext);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rename(src, dst, RenameOptions.defaults());
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenameOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      // TODO(calvin): Update this code on the master side.
      masterClient.rename(src, dst);
      LOG.debug("Renamed {} to {}, options: {}", src.getPath(), dst.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void setAttribute(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    setAttribute(path, SetAttributeOptions.defaults());
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.setAttribute(path, options);
      LOG.debug("Set attributes for {}, options: {}", path.getPath(), options);
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void unmount(AlluxioURI path) throws IOException, AlluxioException {
    unmount(path, UnmountOptions.defaults());
  }

  @Override
  public void unmount(AlluxioURI path, UnmountOptions options)
      throws IOException, AlluxioException {
    FileSystemMasterClient masterClient = mFileSystemContext.acquireMasterClient();
    try {
      masterClient.unmount(path);
      LOG.debug("Unmounted {}, options: {}", path.getPath(), options);
    } catch (UnavailableException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    } finally {
      mFileSystemContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public FileInStream queryFile(AlluxioURI path, String var, double max, double min,
      boolean augmented)
        throws FileDoesNotExistException, IOException, AlluxioException {
    GetStatusOptions queryOption = GetStatusOptions.defaults();
    OpenFileOptions options = OpenFileOptions.defaults();
    QueryInfo mQuery = new QueryInfo(max, min, var, augmented);
    queryOption.setQueryInfo(mQuery);
    URIStatus status = getStatus(path, queryOption);
    if (status.isFolder()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(status.getName()));
    }
    InStreamOptions inStreamOptions = options.toInStreamOptions();
    FileInStream instream = FileInStream.create(status, inStreamOptions, mFileSystemContext);
    instream.setQueryLength();
    return instream;
  }

  @Override
  public Set<String> selectValues(List<String> keylist, List<String> valuelist,
      List<String> typelist) throws Exception {
    //String storepath = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    //HashStore sValueStore = new HashStore(new File(storepath.concat("/ValueStore")), 10240);
    Set<String> ret = new HashSet();
    Set<String> keyset = new HashSet();
    Set<String> tmpvalueset = new HashSet();
    boolean hasRepeatedKey = false;
    for (int i = 0; i < keylist.size(); i++) {
      String tmpkey = keylist.get(i);
      if (!keyset.add(tmpkey)) {
        hasRepeatedKey = true;
      }
      Set<String> valueset = sValueStore.get(tmpkey);
      Iterator<String> iterator = valueset.iterator();
      if (valueset == null) {
        continue;
      }
      String currentvalue = valuelist.get(i);
      String selecttype = typelist.get(i);
      switch (selecttype) {
        case "eq" : {
          if (valueset.contains(currentvalue)) {
            ret.add(tmpkey.concat(currentvalue));
          }
          break;
        } //end case "eq"
        case "lt" : {
          if (!isNumeric(currentvalue)) {
            break;
          }
          double cvalue = Double.valueOf(currentvalue);
          while (iterator.hasNext()) {
            String tmpvalue = iterator.next();
            if (isNumeric(tmpvalue)) {
              double ivalue = Double.valueOf(tmpvalue);
              if (ivalue >= cvalue) {
                if (!hasRepeatedKey) {
                  tmpvalueset.add(tmpkey.concat(tmpvalue));
                } else {
                  if (tmpvalueset.contains(tmpkey.concat(tmpvalue))) {
                    ret.add(tmpkey.concat(tmpvalue));
                  }
                }
              }
            } else {
              continue;
            }
          }
          break;
        } //end case "lt"
        case "st" : {
          if (!isNumeric(currentvalue)) {
            break;
          }
          double cvalue = Double.valueOf(currentvalue);
          while (iterator.hasNext()) {
            String tmpvalue =  iterator.next();
            if (isNumeric(tmpvalue)) {
              double ivalue = Double.valueOf(tmpvalue);
              if (ivalue <= cvalue) {
                if (!hasRepeatedKey) {
                  tmpvalueset.add(tmpkey.concat(tmpvalue));
                } else {
                  if (tmpvalueset.contains(tmpkey.concat(tmpvalue))) {
                    ret.add(tmpkey.concat(tmpvalue));
                  }
                }
              }
            } else {
              continue;
            }
          }
          break;
        } //end case "st"
        default : {
          System.out.println("Select type error");
        }
      } //End switch
    } // End for
    if (!hasRepeatedKey) { //only lt or st seceltion
      ret.addAll(tmpvalueset);
    }
    keyset = null;
    tmpvalueset = null;
    return ret;
  }

  @Override
  public Set<String> selectPaths(Set<String> keylist) throws Exception {
    //String storepath = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    //HashStore sPathStore = new HashStore(new File(storepath.concat("/PathStore")), 10240);
    Set<String> ret = new HashSet();
    Iterator<String> iterator = keylist.iterator();
    while (iterator.hasNext()) {
      Set<String> pathset = sPathStore.get(iterator.next());
      if (pathset != null) {
        ret.addAll(pathset);
      }
    }
    return ret;
  }

  /**
   * Whether the input String is numeric.
   * @param str the input String
   */
  private boolean isNumeric(String str) {
    int i;
    for (i = 0; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Set<String> mpiSetect(List<String> keylist, List<String> valuelist, List<String> typelist,
      int mpisize, int mpirank) throws Exception {
    Set<String> ret = new HashSet();
    //String storepath = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
    //HashStore sPathStore = new HashStore(new File(storepath.concat("/PathStore")), 10240);
    Set<String> pathkeyset = null;
    int i;
    try {
      pathkeyset = selectValues(keylist, valuelist, typelist);
    } catch (Exception e) {
      System.out.println("Get path key error!");
    }
    if (pathkeyset == null) {
      System.out.println("No satisfied KV pairs in Hash Store");
      return null;
    } else {
      Object[] pathkeys = pathkeyset.toArray();
      for (i = mpirank; i < pathkeys.length; i += mpisize) {
        Set<String> tmppathset = sPathStore.get((String) pathkeys[i]);
        if (tmppathset != null) {
          ret.addAll(tmppathset);
        }
      }
    }
    return ret;
  }

  @Override
  public void addDatasetInfo(String name, List<String> keys, List<String> values) {
    HDFDataSet tmpinfo = new HDFDataSet(name);
    Map<String, String> attributes = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      attributes.put(keys.get(i), values.get(i));
    }
    tmpinfo.setUDM(attributes);
    sDatasetInfo.add(tmpinfo);
  }

  @Override
  public void setDatasetInfo(AlluxioURI path) throws Exception {
    try {
      SetAttributeOptions soptions = SetAttributeOptions.defaults();
      soptions.addH5(sDatasetInfo);
      setAttribute(path, soptions);
      sDatasetInfo.clear();
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void setDataAccessPattern(String pattern, int tier, String target, long blockSize)
      throws Exception {
    String patternName = pattern.toLowerCase();
    FileWriteLocationPolicy localgetLocationPolicy = new RoundRobinPolicy();
    try {
      switch (patternName) {
        case  "pipeline" : {
          localgetLocationPolicy = new LocalFirstPolicy();
          sCreateFileOption.setLocationPolicy(localgetLocationPolicy);
          sCreateFileOption.setBlockSizeBytes(512 * 1024 * 1024);
          sCreateFileOption.setWriteTier(0);
          LOG.info("Set pipeline pattern");
          break;
        }
        case "scatter" : {
          localgetLocationPolicy = new RoundRobinPolicy();
          sCreateFileOption.setLocationPolicy(localgetLocationPolicy);
          sCreateFileOption.setBlockSizeBytes(blockSize);
          //int storageTier = getLoadBalanceStrategy("MaxFree");
          sCreateFileOption.setWriteTier(tier);
          LOG.info("Set multicast pattern");
          break;
        }
        case "gather" :
        case "reduce" : {
          localgetLocationPolicy = new SpecificHostPolicy(target);
          sCreateFileOption.setLocationPolicy(localgetLocationPolicy);
          sCreateFileOption.setBlockSizeBytes(512 * 1024 * 1024);
          //int storageTier = getLoadBalanceStrategy("MaxFree");
          sCreateFileOption.setWriteTier(tier);
          LOG.info("Set gather pattern");
          break;
        }
        case "multicast" : {
          localgetLocationPolicy = new RoundRobinPolicy();
          sCreateFileOption.setLocationPolicy(localgetLocationPolicy);

          if (blockSize < 1024 * 1024) {
            sCreateFileOption.setWriteTier(0);
          } else {
            //int storageTier = getLoadBalanceStrategy("MaxFree");
            sCreateFileOption.setWriteTier(tier);
          }
          LOG.info("Set multicast pattern");
          break;
        }
        default : {
          UserDefinedPatterns tmpPattern = sPatternStore.get(patternName);
          if (tmpPattern != null) {
            sCreateFileOption.setBlockSizeBytes(
                tmpPattern.getBlockSizeBytes());
            switch (tmpPattern.getLocationPolicyClass()) {
              case "RoundRobinPolicy" : {
                localgetLocationPolicy = new RoundRobinPolicy();
                break;
              }
              case "LocalFirstPolicy" : {
                localgetLocationPolicy = new LocalFirstPolicy();
                break;
              }
              case "SpecificHostPolicy" : {
                localgetLocationPolicy = new SpecificHostPolicy(tmpPattern.getHost());
                break;
              }
              default : {
                LOG.debug("Unknown layout policy");
              }
            }
            sCreateFileOption.setLocationPolicy(localgetLocationPolicy);
            sCreateFileOption.setWriteTier(tier);
            /*storage tier defined by user has higher prioity than LoadBalanceStrategy*/
            sCreateFileOption.setWriteTier(tmpPattern.getWriteTier());
            LOG.info("Set " + tmpPattern + "pattern");
          } else {
            LOG.info("Unknown Data Access Pattern error");
          }
        }
      }
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void defineDataAccessPattern(String pattern) throws Exception {
    UserDefinedPatterns tmpPattern = UserDefinedPatterns.defaults();
    sPatternStore.put(pattern.toLowerCase(), tmpPattern);
    LOG.info("Add data access pattern" + pattern);
  }

  @Override
  public void setStorageTier(String pattern, int tier) throws Exception {
    UserDefinedPatterns tmpPattern = sPatternStore.get(pattern.toLowerCase());
    if (tmpPattern != null) {
      tmpPattern.setWriteTier(tier);
      sPatternStore.put(pattern.toLowerCase(), tmpPattern);
      LOG.info("Set storage tier to " + tier + "for pattern " + pattern);
    }
  }

  @Override
  public void setBlockSize(String pattern, long blockSizeBytes) throws Exception {
    UserDefinedPatterns tmpPattern = sPatternStore.get(pattern.toLowerCase());
    if (tmpPattern != null) {
      tmpPattern.setBlockSizeBytes(blockSizeBytes);
      sPatternStore.put(pattern.toLowerCase(), tmpPattern);
      LOG.info("Set block size to " + blockSizeBytes + "for pattern " + pattern);
    }
  }

  @Override
  public void setLayoutStrategy(String pattern, String layoutStrategy) throws Exception {
    UserDefinedPatterns tmpPattern = sPatternStore.get(pattern.toLowerCase());
    if (tmpPattern != null) {
      tmpPattern.setLocationPolicy(layoutStrategy);
      sPatternStore.put(pattern.toLowerCase(), tmpPattern);
      LOG.info("Set layout strategy to " + layoutStrategy + "for pattern " + pattern);
    }
  }

  @Override
  public void setLoadBalanceStrategy(String pattern, String loadBalance) throws Exception {
    UserDefinedPatterns tmpPattern = sPatternStore.get(pattern.toLowerCase());
    if (tmpPattern != null) {
      tmpPattern.setLoadBalanceStrategy(loadBalance);
      sPatternStore.put(pattern.toLowerCase(), tmpPattern);
      LOG.info("Set balance strategy to " + loadBalance + "for pattern " + pattern);
    }
  }

  @Override
  public void setHost(String pattern, String host) throws Exception {
    UserDefinedPatterns tmpPattern = sPatternStore.get(pattern.toLowerCase());
    if (tmpPattern != null) {
      tmpPattern.setHost(host);
      sPatternStore.put(pattern.toLowerCase(), tmpPattern);
      LOG.info("Set host to " + host + "for pattern " + pattern);
    }
  }

  /*
  @Overrid
  public int getLoadBalanceStrategy(String myloadbalance) {
    int mytier = 0;
    switch (myloadbalance) {
      case "MaxFree" : {
        BlockMetadataManager metaManager = BlockMetadataManager.createBlockMetadataManager();
        StorageTier tier0 = metaManager.getTier("MEM");
        StorageTier tier1 = metaManager.getTier("HDD");
        long freeTier0 = tier0.getAvailableBytes();
        long avaTier0 = tier0.getCapacityBytes();
        long freeTier1 = tier1.getAvailableBytes();
        long avaTier1 = tier1.getCapacityBytes();
        double f0 = ((double) freeTier0) / ((double) avaTier0);
        double f1 = ((double) freeTier1) / ((double) avaTier1);
        if (f0 < f1) {
          mytier = 1;
        } else {
          mytier = 0;
        }
        break;
      }
      case "RoundRobin" : {
        sNum++;
        if (sNum == 1000) {
          sNum = 0;
        }
        mytier = (sNum - 1) % 2;
        break;
      }
      case "Random" : {
        mytier = (int) (Math.random() * 2);
        break;
      }
      default : {
        LOG.debug("Unknown LoadBalanceStrategy error");
      }
    }
    return mytier;
  }
  */

}
