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

import alluxio.Configuration;
import alluxio.PropertyKey;

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
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * The DataBase that handles block index info.
 */
public class PathStore implements Closeable {

  private static String sStorepath = Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER);
  protected static final SerializableObjectStore<String, Set<String>> PATHSTORE =
      createDataStore(new File(sStorepath.concat("/PathStore")), 10240);

  /**
   * @return the PathStore
   */
  public static final SerializableObjectStore<String, Set<String>> getDataStore() {
    return PATHSTORE;
  }

  /**
   * Create the PathStore.
   * @param homeDir the directroy that stores the hash data base
   * @param initialCapacity the initial Capacity of the hash data base
   */
  protected static SerializableObjectStore<String, Set<String>>
      createDataStore(File homeDir, int initialCapacity) {
    SerializableObjectStore objectstore = null;
    try {
      StoreConfig config = new StoreConfig(homeDir, initialCapacity);
      config.setSegmentFactory(new MemorySegmentFactory());
      config.setSegmentFileSizeMB(64);
      DynamicDataStore tmpstore = StoreFactory.createDynamicDataStore(config);
      objectstore = new SerializableObjectStore(tmpstore,
          new StringSerializer(), new JavaSerializer<HashSet>());
    } catch (Exception e) {
      System.out.println("Init Path Store failed");
    }
    return objectstore;
  }

  /**
   * Put the valueset corresponding to each key.
   * @param ikey the input key
   * @param valuelist the value list
   */
  public void putUDMKey(List<String> ikey, List<String> valuelist) throws Exception {
    int i;
    for (i = 0; i < ikey.size(); i++) {
      String tmpkey = ikey.get(i);
      if (PATHSTORE.get(tmpkey) != null) {
        Set<String> currentvalue = PATHSTORE.get(tmpkey);
        currentvalue.add(valuelist.get(i));
        PATHSTORE.put(tmpkey, currentvalue);
      } else {
        Set<String> tmpvalue = new HashSet();
        tmpvalue.add(valuelist.get(i));
        PATHSTORE.put(tmpkey, tmpvalue);
      }
    }
  }

  /**
   * Put the UDM Path.
   * @param tvalue the key of each path
   * @param pathset the path list
   */
  public void putUDMPath(String tvalue, Set<String> pathset) throws Exception {
    if (PATHSTORE.get(tvalue) != null) {
      Set<String> currentpath = PATHSTORE.get(tvalue);
      Iterator<String> iterator = pathset.iterator();
      while (iterator.hasNext()) {
        currentpath.add(iterator.next());
      }
      PATHSTORE.put(tvalue, currentpath);
    } else {
      PATHSTORE.put(tvalue, pathset);
    }
  }

  /**
   * Delete the key value.
   * @param ikey the input key
   * @param valuelist the input value
   */
  public void deleteUDMKey(List<String> ikey, List<String> valuelist) throws Exception {
    int i;
    for (i = 0; i < ikey.size(); i++) {
      String tmpkey = ikey.get(i);
      if (PATHSTORE.get(tmpkey) != null) {
        Set<String> currentvalue = PATHSTORE.get(tmpkey);
        currentvalue.remove(valuelist.get(i));
        PATHSTORE.put(tmpkey, currentvalue);
      }
    }
  }

  /**
   * Delete the UDM Path.
   * @param tvalue the key of each path
   * @param pathset the path list
   */
  public void deleteUDMPath(String tvalue, Set<String> pathset) throws Exception {
    if (PATHSTORE.get(tvalue) != null) {
      Set<String> currentpath = PATHSTORE.get(tvalue);
      Iterator<String> iterator = pathset.iterator();
      while (iterator.hasNext()) {
        currentpath.remove(iterator.next());
      }
      PATHSTORE.put(tvalue, currentpath);
    }
  }

  /**
   * Get the value of target key.
   * @param tkey the target key
   * @return the target value
   */
  public Set<String> get(String tkey) {
    return PATHSTORE.get(tkey);
  }

  /**
   * Sync the hash data base.
   */
  public void sync() throws Exception {
    PATHSTORE.sync();
  }

  @Override
  public boolean isOpen() {
    return PATHSTORE.isOpen();
  }

  @Override
  public void open() throws IOException {
    PATHSTORE.open();
  }

  /**
   * Close the data base.
   */
  public void close() throws IOException {
    PATHSTORE.close();
  }

}
