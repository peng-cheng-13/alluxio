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

import alluxio.wire.IndexInfo;

import java.util.concurrent.Callable;

/**
 * Multi-thread that generate index value.
 */
public class MyCallable implements Callable<Object> {
  private byte[] mInput;
  private int mOff;
  private int mTmpstride;
  private int mStart;
  private int mEnd;
  private String mVartype;
  private int mTmpoff;
  private double mCurrentmin;
  private boolean mAugmented;
  private double mAugMin;
  private double mAugRange;
  private long mAugBitmap;

  /**
   * Create a new instance of thread.
   * @param b the input array
   * @param off the initial offset
   * @param tmpstride the stride of array
   * @param start the start offset
   * @param end the end offset
   * @param vartype the var type
   * @param tmpoff the offset of each var
   * @param tmpvalue current min value
   * @param augmented whether to bit augmented index
   * @param tmpmin the min value of augmented index
   * @param tmprange the range of augmented index
   * @param bitmap the bitmap of augmented index
   */
  protected MyCallable(byte[] b, int off, int tmpstride, int start, int end, String vartype,
      int tmpoff, double tmpvalue, boolean augmented, double tmpmin, double tmprange, long bitmap) {
    mInput = b;
    mOff = off;
    mTmpstride = tmpstride;
    mStart = start;
    mEnd = end;
    mVartype = vartype;
    mTmpoff = tmpoff;
    mCurrentmin = tmpvalue;
    mAugmented = augmented;
    mAugMin = tmpmin;
    mAugRange = tmprange;
    mAugBitmap = bitmap;
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
      ret |= (b[offset + i] & 0xFF) << (i * 8);
    }
    return ret;
  }
   /**
   * Per thread task.
   * @return the index info
   */
  public IndexInfo call() throws Exception {
    int j;
    double tmpmax = mCurrentmin;
    double tmpmin = mCurrentmin;
    double tmpvalue = mCurrentmin;
    for (j = mStart; j < mEnd; j++) {
      switch (mVartype) {
        case "INT" : {
          tmpvalue = (double) b_to_int(mInput, mOff + j * mTmpstride + mTmpoff);
          break;
        }
        case "FLOAT" : {
          tmpvalue = (double) b_to_float(mInput, mOff + j * mTmpstride + mTmpoff);
          break;
        }
        case "DOUBLE" : {
          tmpvalue = (double) b_to_double(mInput, mOff + j * mTmpstride + mTmpoff);
          break;
        }
        case "LONG" : {
          tmpvalue = (double) b_to_long(mInput, mOff + j * mTmpstride + mTmpoff);
          break;
        }
        case "SHORT" : {
          tmpvalue = (double) b_to_short(mInput, mOff + j * mTmpstride + mTmpoff);
          break;
        }
        default : break;
      }
      if (tmpvalue < tmpmin) {
        tmpmin = tmpvalue;
      }
      if (tmpvalue > tmpmax) {
        tmpmax = tmpvalue;
      }
      //Augmented index
      if (mAugmented) {
        int augindex = ((tmpvalue - mAugMin) < 0) ? 63 :
            (63 - (int) ((tmpvalue - mAugMin) / mAugRange));
        if (augindex < 0) {
          augindex = 0;
        }
        long tmpbitmap = 1L << augindex;
        mAugBitmap = mAugBitmap | tmpbitmap;
      }
    }
    IndexInfo tmpindex = new IndexInfo(1, tmpmax, tmpmin, "tmp");
    tmpindex.setBitmap(mAugBitmap);
    return tmpindex;
  }
}

