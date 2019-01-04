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

package alluxio.master.file;

import alluxio.wire.IndexInfo;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.io.Closeable;
import krati.store.DynamicDataStore;
import krati.store.SerializableObjectStore;
import krati.io.serializer.StringSerializer;
import krati.io.serializer.JavaSerializer;

import java.io.File;
import java.io.IOException;

/**
 * The DataBase that handles block index info.
 */
public class KratiDataStore implements Closeable {

  private final SerializableObjectStore<String, IndexInfo> mHashStore;

  /**
   * Creates a new instance of {@link KratiDataStore}.
   * @param homeDir the directroy that stores the hash data base
   * @param initialCapacity the initial Capacity of the hash data base
   */
  public KratiDataStore(File homeDir, int initialCapacity) throws Exception {
    mHashStore = createDataStore(homeDir, initialCapacity);
  }

  /**
   * @return the KratiDataStore
   */
  public final SerializableObjectStore<String, IndexInfo> getDataStore() {
    return mHashStore;
  }

  /**
   * Create the KratiDataStore.
   * @param homeDir the directroy that stores the hash data base
   * @param initialCapacity the initial Capacity of the hash data base
   */
  protected SerializableObjectStore<String, IndexInfo>
      createDataStore(File homeDir, int initialCapacity) throws Exception {
    StoreConfig config = new StoreConfig(homeDir, initialCapacity);
    config.setSegmentFactory(new MemorySegmentFactory());
    config.setSegmentFileSizeMB(64);
    DynamicDataStore tmpstore = StoreFactory.createDynamicDataStore(config);
    SerializableObjectStore objectstore = new SerializableObjectStore(tmpstore,
        new StringSerializer(), new JavaSerializer<IndexInfo>());
    return objectstore;
  }

  /**
   * Put the key value.
   * @param ikey the input key
   * @param ivalue the input value
   */
  public void put(String ikey, IndexInfo ivalue) throws Exception {
    mHashStore.put(ikey, ivalue);
  }

  /**
   * Get the value of target key.
   * @param tkey the target key
   * @return the target value
   */
  public IndexInfo get(String tkey) {
    return mHashStore.get(tkey);
  }

  /**
   * Sync the hash data base.
   */
  public void sync() throws Exception {
    mHashStore.sync();
  }

  @Override
  public boolean isOpen() {
    return mHashStore.isOpen();
  }

  @Override
  public void open() throws IOException {
    mHashStore.open();
  }

  /**
   * Close the data base.
   */
  public void close() throws IOException {
    mHashStore.close();
  }

}
