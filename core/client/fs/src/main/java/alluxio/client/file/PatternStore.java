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
public class PatternStore implements Closeable {

  private final SerializableObjectStore<String, UserDefinedPatterns> mPatternStore;

  /**
   * Creates a new instance of {@link PatternStore}.
   * @param homeDir the directroy that stores the hash data base
   * @param initialCapacity the initial Capacity of the hash data base
   */
  public PatternStore(File homeDir, int initialCapacity) throws Exception {
    mPatternStore = createDataStore(homeDir, initialCapacity);
  }

  /**
   * @return the PatternStore
   */
  public final SerializableObjectStore<String, UserDefinedPatterns> getDataStore() {
    return mPatternStore;
  }

  /**
   * Create the PatternStore.
   * @param homeDir the directroy that stores the hash data base
   * @param initialCapacity the initial Capacity of the hash data base
   */
  protected SerializableObjectStore<String, UserDefinedPatterns>
      createDataStore(File homeDir, int initialCapacity) throws Exception {
    StoreConfig config = new StoreConfig(homeDir, initialCapacity);
    config.setSegmentFactory(new MemorySegmentFactory());
    config.setSegmentFileSizeMB(64);
    DynamicDataStore tmpstore = StoreFactory.createDynamicDataStore(config);
    SerializableObjectStore objectstore = new SerializableObjectStore(tmpstore,
        new StringSerializer(), new JavaSerializer<UserDefinedPatterns>());
    return objectstore;
  }

  /**
   * Put the UserDefinedPatterns.
   * @param ikey the name of user defined pattern
   * @param value the value of user defined pattern
   */
  public void put(String ikey, UserDefinedPatterns value) throws Exception {
    mPatternStore.put(ikey, value);
  }

  /**
   * Delete the UserDefinedPatterns.
   * @param ikey the name of user defined pattern
   */
  public void deleteUDMPath(String ikey) throws Exception {
    mPatternStore.delete(ikey);
  }

  /**
   * Get the value of target key.
   * @param tkey the target key
   * @return the target value
   */
  public UserDefinedPatterns get(String tkey) {
    return mPatternStore.get(tkey);
  }

  /**
   * Sync the hash data base.
   */
  public void sync() throws Exception {
    mPatternStore.sync();
  }

  @Override
  public boolean isOpen() {
    return mPatternStore.isOpen();
  }

  @Override
  public void open() throws IOException {
    mPatternStore.open();
  }

  /**
   * Close the data base.
   */
  public void close() throws IOException {
    mPatternStore.close();
  }

}
