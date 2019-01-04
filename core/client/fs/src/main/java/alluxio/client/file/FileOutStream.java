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
import alluxio.wire.AugIndex;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;

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
  private SetAttributeOptions mIndexOptions;
  private int mStride;

  private boolean mCanceled;
  private boolean mClosed;
  private boolean mShouldCacheCurrentBlock;
  private boolean mShouldIndex;
  private boolean mAugmented;
  private boolean mFirstBlock;
  private HashMap<String, Double> mVar2Min;
  private HashMap<String, Double> mVar2Max;
  private HashMap<String, String> mVar2Type;
  private HashMap<String, Integer> mVarOffset;
  private HashMap<String, AugIndex> mAugIndex;
  private HashMap<String, Long> mVar2Bitmap;
  private BlockOutStream mCurrentBlockOutStream;
  private List<BlockOutStream> mPreviousBlockOutStreams;
  private int mTaskSize;
  private long mBlockLeftSize;
  private double mIndexComputeTime;
  private double mIndexWriteTime;

  private List<String> mVar;
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
    mIndexOptions = SetAttributeOptions.defaults();
    mContext = context;
    mBlockStore = AlluxioBlockStore.create(mContext);
    mPreviousBlockOutStreams = new LinkedList<>();
    mClosed = false;
    mCanceled = false;
    mShouldCacheCurrentBlock = mAlluxioStorageType.isStore();
    mBytesWritten = 0;
    mShouldIndex = false;
    mAugmented = false;
    mFirstBlock = true;
    mVar2Min = new HashMap<String, Double>();
    mVar2Max = new HashMap<String, Double>();
    mVar2Type = new HashMap<String, String>();
    mVarOffset = new HashMap<String, Integer>();
    mAugIndex = new HashMap<String, AugIndex>();
    mVar2Bitmap = new HashMap<String, Long>();
    mVar = new ArrayList<String>();
    mTaskSize = 1;
    mBlockLeftSize = 0;
    mIndexComputeTime = 0;
    mIndexWriteTime = 0;

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
      addIndexInfo();
      long startTime = System.currentTimeMillis();
      sentIndexInfo();
      long endTime = System.currentTimeMillis();
      mIndexWriteTime = (double) (endTime - startTime);
      mVar2Min = null;
      mVar2Max = null;
      mVar2Type = null;
      mVarOffset = null;
      mAugIndex = null;
      mVar2Bitmap = null;
      mVar = null;
      mIndexOptions = null;
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
              addIndexInfo();
            }
            getNextBlock();
            //Init MinMax value of current block
            if (mShouldIndex) {
              initMinMax(b, tOff, tLen);
            }
          }
          long currentBlockLeftBytes = mCurrentBlockOutStream.remaining();
          if (currentBlockLeftBytes >= tLen) {
            if (mShouldIndex) {
              long startTime = System.currentTimeMillis();
              getMinMax(b, tOff, tLen);
              long endTime = System.currentTimeMillis();
              mIndexComputeTime += (double) (endTime - startTime);
            } else {
              mCurrentBlockOutStream.write(b, tOff, tLen);
            }
            tLen = 0;
          } else {
            if (mShouldIndex) {
              long startTime = System.currentTimeMillis();
              getMinMax(b, tOff, (int) currentBlockLeftBytes);
              long endTime = System.currentTimeMillis();
              mIndexComputeTime += (double) (endTime - startTime);
            } else {
              mCurrentBlockOutStream.write(b, tOff, (int) currentBlockLeftBytes);
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
      int tLen = len;
      int tOff = off;
      try {
        while (tLen > 0) {
          if (mBlockLeftSize == 0) {
            if (mBytesWritten != 0 && mShouldIndex) {
              addIndexInfo();
            }
            getNextBlockId();
            mBlockLeftSize = mBlockSize;
            if (mShouldIndex) {
              initMinMax(b, tOff, tLen);
            }
          }
          long currentBlockLeftBytes = mBlockLeftSize;
          if (currentBlockLeftBytes >= tLen) {
            if (mShouldIndex) {
              long startTime = System.currentTimeMillis();
              getMinMax(b, tOff, tLen);
              long endTime = System.currentTimeMillis();
              mIndexComputeTime += (double) (endTime - startTime);
            } else {
              mUnderStorageOutputStream.write(b, tOff, tLen);
            }
            mBlockLeftSize -= tLen;
            tLen = 0;
          } else {
            if (mShouldIndex) {
              long startTime = System.currentTimeMillis();
              getMinMax(b, tOff, (int) currentBlockLeftBytes);
              long endTime = System.currentTimeMillis();
              mIndexComputeTime += (double) (endTime - startTime);
            } else {
              mUnderStorageOutputStream.write(b, tOff, (int) currentBlockLeftBytes);
            }
            mBlockLeftSize -= currentBlockLeftBytes;
            tOff += currentBlockLeftBytes;
            tLen -= currentBlockLeftBytes;
          }
        }
      } catch (Exception e) {
        handleCacheWriteException(e);
      }
      //mUnderStorageOutputStream.write(b, off, len);
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

      //Init bitmap for each block
      int i;
      String tmpvar;
      for (i = 0; i < mVar.size(); i++) {
        tmpvar = mVar.get(i);
        if (mAugmented) {
          mVar2Bitmap.put(tmpvar, (long) 0x0000000000000000L);
        } else {
          mVar2Bitmap.put(tmpvar, (long) 0xFFFFFFFFFFFFFFFFL);
        }
      }
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
  protected void setFileInfo(ArrayList<String> varname, ArrayList<String> vartype)
      throws IOException {
    int i = 0;
    // Length of 'short' is 2
    int tmpstride = 2;
    String tmpvar;
    String tmptype;
    if (varname.size() != vartype.size()) {
      throw new IOException("Wrong var info!!!");
    }
    for (i = 0; i < varname.size(); i++) {
      tmpvar = varname.get(i);
      tmptype = vartype.get(i);
      mVar.add(tmpvar);
      mVar2Type.put(tmpvar, tmptype);
      switch (tmptype) {
        case "SHORT" : {
          mVarOffset.put(tmpvar, i);
          tmpstride = 2;
          break;
        }
        case "FLOAT" :
        case "INT" : {
          mVarOffset.put(tmpvar, i);
          tmpstride = 4;
          break;
        }
        case "LONG" :
        case "DOUBLE" : {
          mVarOffset.put(tmpvar, i);
          tmpstride = 8;
          break;
        }
        default : {
          LOG.warn("Type Error");
        }
      }
    }
    mStride = tmpstride;
  }

  /**
   * Generate index.
   * @param tasknum the number of task
   * @param augmented whether to set augmented index info
   */
  public void buildIndex(int tasknum, boolean augmented) throws IOException {
    mShouldIndex = true;
    if (mVar.size() == 0) {
      throw new IOException("File info is null!");
    }
    if (tasknum > 1) {
      if (tasknum > 24) {
        mTaskSize = 24;
      } else {
        mTaskSize = tasknum;
      }
    }
    //Augmented index
    mAugmented = augmented;
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
    int i;
    int tmpoff;
    String tmpvar;
    String tmpvartype;
    if (len < mStride * mVarOffset.size()) {
      LOG.error("Incomplete lenth");
    } else {
      for (i = 0; i < mVar.size(); i++) {
        tmpvar = mVar.get(i);
        tmpvartype = mVar2Type.get(tmpvar);
        tmpoff = mVarOffset.get(tmpvar);
        //mVar2Bitmap.put(tmpvar, (long) 0x0000);
        switch (tmpvartype) {
          case "INT" : {
            double tmpvalue = (double) b_to_int(b, off + tmpoff * mStride);
            mVar2Max.put(tmpvar, tmpvalue);
            mVar2Min.put(tmpvar, tmpvalue);
            break;
          }
          case "FLOAT" : {
            double tmpvalue = (double) b_to_float(b, off + tmpoff * mStride);
            mVar2Max.put(tmpvar, tmpvalue);
            mVar2Min.put(tmpvar, tmpvalue);
            break;
          }
          case "DOUBLE" : {
            double tmpvalue = (double) b_to_double(b, off + tmpoff * mStride);
            mVar2Max.put(tmpvar, tmpvalue);
            mVar2Min.put(tmpvar, tmpvalue);
            break;
          }
          case "LONG" : {
            double tmpvalue = (double) b_to_long(b, off + tmpoff * mStride);
            mVar2Max.put(tmpvar, tmpvalue);
            mVar2Min.put(tmpvar, tmpvalue);
            break;
          }
          case "SHORT" : {
            double tmpvalue = (double) b_to_short(b, off + tmpoff * mStride);
            mVar2Max.put(tmpvar, tmpvalue);
            mVar2Min.put(tmpvar, tmpvalue);
            break;
          }
          default : break;
        }
      }
    }
  }

  /**
   * Get max value of byte array.
   * @param b the byte array
   * @param off the offset of byte array
   * @param len the length of byte array
   */
  private void getMinMax(byte[] b, int off, int len)
      throws InterruptedException, ExecutionException, IOException {
    int tmpstride = mStride * mVarOffset.size();
    int count = len / tmpstride;
    int i;
    int j;
    int start;
    int end;
    int tmpoff = 0;
    double currentmax;
    double currentmin;
    //Augment index info
    double firstmin;
    double range;
    long currentbitmap;
    String tmpvar;
    String tmpvartype;
    for (i = 0; i < mVar.size(); i++) {
      //Init the name, type, currentmax, currentmin value
      tmpvar = mVar.get(i);
      tmpvartype = mVar2Type.get(tmpvar);
      tmpoff = mVarOffset.get(tmpvar) * mStride;
      currentmax = mVar2Max.get(tmpvar);
      currentmin = mVar2Min.get(tmpvar);
      double tmpvalue = mVar2Min.get(tmpvar);
      //Augment index info
      if (mAugmented && (!mFirstBlock)) {
        firstmin = mAugIndex.get(tmpvar).mMin;
        range = mAugIndex.get(tmpvar).mRange;
      } else {
        firstmin = 0;
        range = 1;
      }
      currentbitmap = mVar2Bitmap.get(tmpvar);
      //Init multithread
      ExecutorService pool = Executors.newFixedThreadPool(mTaskSize);
      List<Future> list = new ArrayList<Future>();
      for (j = 0; j < mTaskSize; j++) {
        start = j * count / mTaskSize;
        if (j == (mTaskSize - 1)) {
          end = count;
        } else {
          end = (j + 1) * count / mTaskSize;
        }
        Callable c = new MyCallable(b, off, tmpstride, start, end, tmpvartype,
            tmpoff, tmpvalue, mAugmented, firstmin, range, currentbitmap);
        Future f = pool.submit(c);
        list.add(f);
      }
      //Write data to each block
      if (i == 0) {
        if (mShouldCacheCurrentBlock) {
          mCurrentBlockOutStream.write(b, off, len);
        } else {
          mUnderStorageOutputStream.write(b, off, len);
        }
      }
      pool.shutdown();
      //Get max and min value
      long tmpbitmap = 0xFFFFFFFFFFFFFFFFL;
      for (Future f : list) {
        double minvalue = ((IndexInfo) f.get()).getMinValue();
        double maxvalue = ((IndexInfo) f.get()).getMaxValue();
        tmpbitmap = ((IndexInfo) f.get()).getBitmap();
        if (minvalue < currentmin) {
          currentmin = minvalue;
        }
        if (maxvalue > currentmax) {
          currentmax = maxvalue;
        }
      }
      mVar2Min.put(tmpvar, currentmin);
      mVar2Max.put(tmpvar, currentmax);
      mVar2Bitmap.put(tmpvar, tmpbitmap);
      /*
      for (j = 0; j < count; j++) {
        //Get tmpvalue from byte array
        switch (tmpvartype) {
          case "INT" : {
            tmpvalue = (double) b_to_int(b, off + j * tmpstride + tmpoff);
            break;
          }
          case "FLOAT" : {
            tmpvalue = (double) b_to_float(b, off + j * tmpstride + tmpoff);
            break;
          }
          case "DOUBLE" : {
            tmpvalue = (double) b_to_double(b, off + j * tmpstride + tmpoff);
            break;
          }
          case "LONG" : {
            tmpvalue = (double) b_to_long(b, off + j * tmpstride + tmpoff);
            break;
          }
          case "SHORT" : {
            tmpvalue = (double) b_to_short(b, off + j * tmpstride + tmpoff);
            break;
          }
          default : break;
        }
        //Comparing the tmpvalue with current min/max value
        if (tmpvalue < currentmin) {
          mVar2Min.put(tmpvar, tmpvalue);
        }
        if (currentmax < tmpvalue) {
          mVar2Max.put(tmpvar, tmpvalue);
        }
      } //for (j = 0; j < count; j++)
      */
    } //for (i = 0; i < mVar.size(); i++)
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
    //return mVar2Max.get(mVar.get(0));
    return mIndexComputeTime;
  }

  /**
   * Get min value of current block.
   * @return mCurrentBlockMin
   */
  public double getBlockMin() {
    //return mVar2Min.get(mVar.get(0));
    return mIndexWriteTime;
  }

  /**
   * Sent index info to master.
   */
  private void sentIndexInfo() throws IOException {
    CloseableResource<FileSystemMasterClient> masterClient =
          mContext.acquireMasterClientResource();
    masterClient.get().setAttribute(mUri, mIndexOptions);
    mContext.releaseMasterClient(masterClient.get());
  }

  /**
   * Add index info.
   */
  private void addIndexInfo() {
    //Add current block index info
    int i;
    String tmpvar;
    double tmpmax;
    double tmpmin;
    long tmpbitmap;
    for (i = 0; i < mVar.size(); i++) {
      tmpvar = mVar.get(i);
      tmpmax = mVar2Max.get(tmpvar);
      tmpmin = mVar2Min.get(tmpvar);
      //Init Augmented Minmax Index
      if (mFirstBlock) {
        double tmprange = ((tmpmax - tmpmin) == 0) ? 1 : ((tmpmax - tmpmin) / 64);
        AugIndex tmpindex = new AugIndex(tmpvar, tmpmin, tmprange);
        mAugIndex.put(tmpvar, tmpindex);
      }
      if ((!mFirstBlock) && mAugmented) {
        tmpbitmap = mVar2Bitmap.get(tmpvar);
      } else {
        tmpbitmap = 0xFFFFFFFFFFFFFFFFL;
      }
      mIndexOptions.addBlockIndex(mCurrentBlockId, tmpmax, tmpmin, tmpvar, tmpbitmap);
    }
    mFirstBlock = false;
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

