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
import alluxio.annotation.PublicApi;
import alluxio.client.AbstractOutStream;
import alluxio.client.AlluxioStorageType;
import alluxio.client.UnderStorageType;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.block.stream.UnderFileSystemFileOutStream;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.wire.IndexInfo;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides a streaming API to write a file. This class wraps the BlockOutStreams for each of the
 * blocks in the file and abstracts the switching between streams. The backing streams can write to
 * Alluxio space in the local machine or remote machines. If the {@link UnderStorageType} is
 * {@link UnderStorageType#SYNC_PERSIST}, another stream will write the data to the under storage
 * system.
 */
@PublicApi
@NotThreadSafe
public class FileOutStream extends AbstractOutStream {
  private static final Logger LOG = LoggerFactory.getLogger(FileOutStream.class);

  /** Used to manage closeable resources. */
  private final Closer mCloser;
  private final long mBlockSize;
  private long mCurrentBlockId;
  private final AlluxioStorageType mAlluxioStorageType;
  private final UnderStorageType mUnderStorageType;
  private final FileSystemContext mContext;
  private final AlluxioBlockStore mBlockStore;
  /** Stream to the file in the under storage, null if not writing to the under storage. */
  private final UnderFileSystemFileOutStream mUnderStorageOutputStream;
  private final OutStreamOptions mOptions;

  private boolean mCanceled;
  private boolean mClosed;
  private boolean mShouldCacheCurrentBlock;
  private boolean mShouldIndex;
  private double mCurrentBlockMin;
  private double mCurrentBlockMax;
  private String mVarType;
  private BlockOutStream mCurrentBlockOutStream;
  private List<BlockOutStream> mPreviousBlockOutStreams;

  private String mVar;
  protected final AlluxioURI mUri;

  /**
   * Creates a new file output stream.
   *
   * @param path the file path
   * @param options the client options
   * @param context the file system context
   */
  public FileOutStream(AlluxioURI path, OutStreamOptions options, FileSystemContext context)
      throws IOException {
    mCloser = Closer.create();
    mUri = Preconditions.checkNotNull(path, "path");
    mBlockSize = options.getBlockSizeBytes();
    mCurrentBlockId = 0;
    mAlluxioStorageType = options.getAlluxioStorageType();
    mUnderStorageType = options.getUnderStorageType();
    mOptions = options;
    mContext = context;
    mBlockStore = AlluxioBlockStore.create(mContext);
    mPreviousBlockOutStreams = new LinkedList<>();
    mClosed = false;
    mCanceled = false;
    mShouldCacheCurrentBlock = mAlluxioStorageType.isStore();
    mBytesWritten = 0;
    mShouldIndex = false;
    mCurrentBlockMin = 0;
    mCurrentBlockMax = 0;
    mVarType = "Not Defined";
    mVar = "Not Defined";
    if (!mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream = null;
    } else { // Write is through to the under storage, create mUnderStorageOutputStream
      WorkerNetAddress workerNetAddress = // not storing data to Alluxio, so block size is 0
          options.getLocationPolicy().getWorkerForNextBlock(mBlockStore.getWorkerInfoList(), 0);
      if (workerNetAddress == null) {
        // Assume no worker is available because block size is 0
        throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
      }
      try {
        mUnderStorageOutputStream = mCloser
            .register(UnderFileSystemFileOutStream.create(mContext, workerNetAddress, mOptions));
      } catch (Throwable t) {
        throw CommonUtils.closeAndRethrow(mCloser, t);
      }
    }
  }

  @Override
  public void cancel() throws IOException {
    mCanceled = true;
    close();
  }

  @Override
  public void close() throws IOException {
    if (mShouldIndex) {
      sentIndexInfo();
    }
    if (mClosed) {
      return;
    }
    try {
      if (mCurrentBlockOutStream != null) {
        mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
      }

      CompleteFileOptions options = CompleteFileOptions.defaults();
      if (mUnderStorageType.isSyncPersist()) {
        if (mCanceled) {
          mUnderStorageOutputStream.cancel();
        } else {
          mUnderStorageOutputStream.close();
          options.setUfsLength(mBytesWritten);
        }
      }

      if (mAlluxioStorageType.isStore()) {
        if (mCanceled) {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.cancel();
          }
        } else {
          for (BlockOutStream bos : mPreviousBlockOutStreams) {
            bos.close();
          }
        }
      }

      // Complete the file if it's ready to be completed.
      if (!mCanceled && (mUnderStorageType.isSyncPersist() || mAlluxioStorageType.isStore())) {
        try (CloseableResource<FileSystemMasterClient> masterClient = mContext
            .acquireMasterClientResource()) {
          masterClient.get().completeFile(mUri, options);
        }
      }

      if (mUnderStorageType.isAsyncPersist()) {
        scheduleAsyncPersist();
      }
    } catch (Throwable e) { // must catch Throwable
      throw mCloser.rethrow(e); // IOException will be thrown as-is
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  @Override
  public void flush() throws IOException {
    // TODO(yupeng): Handle flush for Alluxio storage stream as well.
    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.flush();
    }
  }

  @Override
  public void write(int b) throws IOException {
    writeInternal(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    writeInternal(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    writeInternal(b, off, len);
  }

  private void writeInternal(int b) throws IOException {
    if (mShouldCacheCurrentBlock) {
      try {
        if (mCurrentBlockOutStream == null || mCurrentBlockOutStream.remaining() == 0) {
          getNextBlock();
        }
        mCurrentBlockOutStream.write(b);
      } catch (IOException e) {
        handleCacheWriteException(e);
      }
    }

    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.write(b);
      Metrics.BYTES_WRITTEN_UFS.inc();
    }
    mBytesWritten++;
  }

  private void writeInternal(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (mShouldCacheCurrentBlock) {
      try {
        int tLen = len;
        int tOff = off;
        while (tLen > 0) {
          if (mCurrentBlockOutStream == null || mCurrentBlockOutStream.remaining() == 0) {
            //Sent MinMax index to Master
            if (mCurrentBlockOutStream != null && mShouldIndex) {
              sentIndexInfo();
            }
            getNextBlock();
            //Init MinMax value of current block
            if (mShouldIndex) {
              initMinMax(b, tOff, tLen);
            }
          }
          long currentBlockLeftBytes = mCurrentBlockOutStream.remaining();
          if (currentBlockLeftBytes >= tLen) {
            mCurrentBlockOutStream.write(b, tOff, tLen);
            if (mShouldIndex) {
              getMinMax(b, tOff, tLen);
            }
            tLen = 0;
          } else {
            mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
            if (mShouldIndex) {
              getMinMax(b, tOff, (int) currentBlockLeftBytes);
            }
            tOff += currentBlockLeftBytes;
            tLen -= currentBlockLeftBytes;
          }
        }
      } catch (Exception e) {
        handleCacheWriteException(e);
      }
    }

    if (mUnderStorageType.isSyncPersist()) {
      mUnderStorageOutputStream.write(b, off, len);
      Metrics.BYTES_WRITTEN_UFS.inc(len);
    }
    mBytesWritten += len;
  }

  private void getNextBlock() throws IOException {
    if (mCurrentBlockOutStream != null) {
      Preconditions.checkState(mCurrentBlockOutStream.remaining() <= 0,
          PreconditionMessage.ERR_BLOCK_REMAINING);
      mCurrentBlockOutStream.flush();
      mPreviousBlockOutStreams.add(mCurrentBlockOutStream);
    }

    if (mAlluxioStorageType.isStore()) {
      mCurrentBlockOutStream =
          mBlockStore.getOutStream(getNextBlockId(), mBlockSize, mOptions);
      mShouldCacheCurrentBlock = true;
    }
  }

  private long getNextBlockId() throws IOException {
    try (CloseableResource<FileSystemMasterClient> masterClient = mContext
        .acquireMasterClientResource()) {
      mCurrentBlockId = masterClient.get().getNewBlockIdForFile(mUri);
      return mCurrentBlockId;
    }
  }

  private void handleCacheWriteException(Exception e) throws IOException {
    LOG.warn("Failed to write into AlluxioStore, canceling write attempt.", e);
    if (!mUnderStorageType.isSyncPersist()) {
      throw new IOException(ExceptionMessage.FAILED_CACHE.getMessage(e.getMessage()), e);
    }

    if (mCurrentBlockOutStream != null) {
      mShouldCacheCurrentBlock = false;
      mCurrentBlockOutStream.cancel();
    }
  }

  /**
   * Schedules the async persistence of the current file.
   */
  protected void scheduleAsyncPersist() throws IOException {
    try (CloseableResource<FileSystemMasterClient> masterClient = mContext
        .acquireMasterClientResource()) {
      masterClient.get().scheduleAsyncPersist(mUri);
    }
  }

  /**
   * Set file info to generate index.
   * @param varname the list contains the var name
   * @param vartype the list contains the var type
   */
  public void setFileInfo(List<String> varname, List<String>vartype) {
    
  } 

  /**
   * Generate index.
   * @param tmpvar the var name
   * @param tmptype the type of var
   */
  public void buildIndex(String tmpvar, String tmptype) {
    mShouldIndex = true;
    if (tmptype == null) {
      LOG.trace("Failed to get type info");
    }
    mVarType = tmptype;
    mVar = tmpvar;
  }

  /**
   * Get int value.
   * @param b the byte array
   * @param offset the offset of byte array
   */
  private int b_to_int(byte[] b, int offset) {
    int i = 0;
    int ret = 0;
    for (i = 0; i < 4; i++) {
      //Big-endian* ret += (b[offset + 4 - i - 1] & 0xFF) << (i * 8);
      ret += (b[offset + i] & 0xFF) << (i * 8);
    }
    return ret;
  }

  /**
   * Get long value.
   * @param b the byte array
   * @param offset the offset of byte array
   */
  private long b_to_long(byte[] b, int offset) {
    int i = 0;
    long ret = 0;
    for (i = 0; i < 8; i++) {
      //Big-endian* ret += (long) (b[offset + 8 - i - 1] & 0xFF) <<  (i * 8);
      ret += (long) (b[offset + i] & 0xFF) <<  (i * 8);
    }
    return ret;
  }

  /**
   * Get double value.
   * @param b the byte array
   * @param offset the offset of byte array
   */
  private double b_to_double(byte[] b, int offset) {
    int i = 0;
    long ret = 0;
    for (i = 0; i < 8; i++) {
      //Big-endian* ret |= ((long) (b[offset + 8 - i - 1] & 0xFF)) << (i * 8);
      ret |= ((long) (b[offset + i] & 0xFF)) << (i * 8);
    }
    double nret = Double.longBitsToDouble(ret);
    return nret;
  }

  /**
   * Get float value.
   * @param b the byte array
   * @param offset the offset of byte array
   */
  private float b_to_float(byte[] b, int offset) {
    int i = 0;
    int ret = 0;
    for (i = 0; i < 4; i++) {
      //Big-endian* ret |= (b[offset + 4 - i - 1] & 0xFF) << (i * 8);
      ret |= (b[offset + i] & 0xFF) << (i * 8);
    }
    float nret = Float.intBitsToFloat(ret);
    return nret;
  }

  /**
   * Get short value.
   * @param b the byte array
   * @param offset the offset of byte array
   */
  private short b_to_short(byte[] b, int offset) {
    int i = 0;
    short ret = 0;
    for (i = 0; i < 2; i++) {
      //Big-endian* ret |= (b[offset + 2 - i - 1] & 0xFF) << (i * 8);
      ret |= (b[offset + i] & 0xFF) << (i * 8);
    }
    return ret;
  }

  /**
   * Init Min/Max value for current block.
   * @param b the byte array
   * @param off the offset of byte array
   * @param len the length of byte array
   */
  private void initMinMax(byte[] b, int off, int len) {
    LOG.warn("Init MinMax value for current block");
    switch (mVarType) {
      case "INT" : {
        if (len < 4) {
          mCurrentBlockMin = 0;
          mCurrentBlockMax = 0;
        } else {
          mCurrentBlockMin = (double) b_to_int(b , off);
          mCurrentBlockMax = (double) b_to_int(b , off);
        }
        break;
      }
      case "FLOAT" : {
        if (len < 4) {
          mCurrentBlockMin = 0;
          mCurrentBlockMax = 0;
        } else {
          mCurrentBlockMin = (double) b_to_float(b , off);
          mCurrentBlockMax = (double) b_to_float(b , off);
        }
        break;
      }
      case "DOUBLE" : {
        if (len < 8) {
          mCurrentBlockMin = 0;
          mCurrentBlockMax = 0;
        } else {
          mCurrentBlockMin = b_to_double(b , off);
          mCurrentBlockMax = b_to_double(b , off);
        }
        break;
      }
      case "LONG" : {
        if (len < 8) {
          mCurrentBlockMin = 0;
          mCurrentBlockMax = 0;
        } else {
          mCurrentBlockMin = (double) b_to_long(b , off);
          mCurrentBlockMax = (double) b_to_long(b , off);
        }
        break;
      }
      case "SHORT" : {
        if (len < 2) {
          mCurrentBlockMin = 0;
          mCurrentBlockMax = 0;
        } else {
          mCurrentBlockMin = (double) b_to_short(b , off);
          mCurrentBlockMax = (double) b_to_short(b , off);
        }
        break;
      }
      default : {
        LOG.warn("Type Error");
      }
    }
  }

  /**
   * Get max value of byte array.
   * @param b the byte array
   * @param off the offset of byte array
   * @param len the length of byte array
   */
  private void getMinMax(byte[] b, int off, int len) {
    LOG.warn("Generate index with var {}, type {}", mVar, mVarType);
    switch (mVarType) {
      case "INT" : {
        int i;
        int count = len / 4;
        int tmpvalue = 0;
        int tmpmin = b_to_int(b , off);
        int tmpmax = b_to_int(b , off);
        for (i = 0; i < count; i++) {
          tmpvalue = b_to_int(b, off + i * 4);
          if (tmpvalue < tmpmin) {
            tmpmin = tmpvalue;
          }
          if (tmpvalue > tmpmax) {
            tmpmax = tmpvalue;
          }
        }
        if ((double) tmpmin < mCurrentBlockMin) {
          mCurrentBlockMin = (double) tmpmin;
        }
        if (mCurrentBlockMax < (double) tmpmax) {
          mCurrentBlockMax = (double) tmpmax;
        }
        break;
      }
      case "FLOAT" : {
        int i;
        int count = len / 4;
        float tmpvalue = 0f;
        float tmpmin = b_to_float(b , off);
        float tmpmax = b_to_float(b , off);
        for (i = 0; i < count; i++) {
          tmpvalue = b_to_float(b, off + i * 4);
          if (tmpvalue < tmpmin) {
            tmpmin = tmpvalue;
          }
          if (tmpvalue > tmpmax) {
            tmpmax = tmpvalue;
          }
        }
        if ((double) tmpmin < mCurrentBlockMin) {
          mCurrentBlockMin = (double) tmpmin;
        }
        if (mCurrentBlockMax < (double) tmpmax) {
          mCurrentBlockMax = (double) tmpmax;
        }
        break;
      }
      case "DOUBLE" : {
        int i;
        int count = len / 8;
        double tmpvalue = 0;
        double tmpmin = b_to_double(b , off);
        double tmpmax = b_to_double(b , off);
        for (i = 0; i < count; i++) {
          tmpvalue = b_to_double(b, off + i * 8);
          if (tmpvalue < tmpmin) {
            tmpmin = tmpvalue;
          }
          if (tmpvalue > tmpmax) {
            tmpmax = tmpvalue;
          }
        }
        if (tmpmin < mCurrentBlockMin) {
          mCurrentBlockMin = tmpmin;
        }
        if (mCurrentBlockMax < tmpmax) {
          mCurrentBlockMax = tmpmax;
        }
        break;
      }
      case  "LONG" : {
        int i;
        int count = len / 8;
        long tmpvalue = 0;
        long tmpmin = b_to_long(b , off);
        long tmpmax = b_to_long(b , off);
        for (i = 0; i < count; i++) {
          tmpvalue = b_to_long(b, off + i * 8);
          if (tmpvalue < tmpmin) {
            tmpmin = tmpvalue;
          }
          if (tmpvalue > tmpmax) {
            tmpmax = tmpvalue;
          }
        }
        if ((double) tmpmin < mCurrentBlockMin) {
          mCurrentBlockMin = (double) tmpmin;
        }
        if (mCurrentBlockMax < (double) tmpmax) {
          mCurrentBlockMax = (double) tmpmax;
        }
        break;
      }
      case "SHORT" : {
        int i;
        int count = len / 2;
        short tmpvalue = 0;
        short tmpmin = b_to_short(b , off);
        short tmpmax = b_to_short(b , off);
        for (i = 0; i < count; i++) {
          tmpvalue = b_to_short(b, off + i * 2);
          if (tmpvalue < tmpmin) {
            tmpmin = tmpvalue;
          }
          if (tmpvalue > tmpmax) {
            tmpmax = tmpvalue;
          }
        }
        if ((double) tmpmin < mCurrentBlockMin) {
          mCurrentBlockMin = (double) tmpmin;
        }
        if (mCurrentBlockMax < (double) tmpmax) {
          mCurrentBlockMax = (double) tmpmax;
        }
        break;
      }
      default : {
        LOG.warn("Type Error");
      }
    }
  }

  /**
   * Whether to generate index.
   * @return mShouldIndex
   */
  public boolean shouldIndex() {
    return mShouldIndex;
  }

  /**
   * Get max value of current block.
   * @return mCurrentBlockMax
   */
  public double getBlockMax() {
    return mCurrentBlockMax;
  }

  /**
   * Get min value of current block.
   * @return mCurrentBlockMin
   */
  public double getBlockMin() {
    return mCurrentBlockMin;
  }

  /**
   * Sent index info to master.
   */
  private void sentIndexInfo() throws IOException {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    IndexInfo mIndexinfo = new IndexInfo();
    mIndexinfo.setBlockId(mCurrentBlockId);
    mIndexinfo.setMinMax(mCurrentBlockMin, mCurrentBlockMax);
    mIndexinfo.setVarName(mVar);
    options.setBlockIndex(mIndexinfo);
    CloseableResource<FileSystemMasterClient> masterClient = mContext.acquireMasterClientResource();
    masterClient.get().setAttribute(mUri, options);
  }

  /**
   * Class that contains metrics about FileOutStream.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter BYTES_WRITTEN_UFS = MetricsSystem.clientCounter("BytesWrittenUfs");

    private Metrics() {} // prevent instantiation
  }
}
