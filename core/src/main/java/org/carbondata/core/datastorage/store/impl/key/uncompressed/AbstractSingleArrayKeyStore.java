/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.datastorage.store.impl.key.uncompressed;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.NodeKeyStore;

public abstract class AbstractSingleArrayKeyStore implements NodeKeyStore {

    /**
     * size of each element
     */
    protected final int sizeOfEachElement;
    /**
     * total number of elements
     */
    protected final int totalNumberOfElements;
    /**
     * data store which will hold the data
     */
    protected byte[] datastore;

    public AbstractSingleArrayKeyStore(int size, int elementSize) {
        this.sizeOfEachElement = elementSize;
        this.totalNumberOfElements = size;
        datastore = new byte[size * elementSize];
    }

    /**
     * This method will be used to insert mdkey to store
     *
     * @param index index of mdkey
     * @param value mdkey
     */
    @Override
    public void put(int index, byte[] value) {
        System.arraycopy(value, 0, datastore, ((index) * sizeOfEachElement), sizeOfEachElement);
    }

    /**
     * This method will be used to get the writable key array.
     * writable key array will hold below information:
     * <size of key array><key array>
     * total length will be stored in 4 bytes+ key array length for key array
     *
     * @return writable array
     */
    @Override
    public byte[] getWritableKeyArray() {
        // create and allocate size for byte buffer
        //  4 bytes for size of array(for array length) + size of array(for array)
        return datastore;
    }

    /**
     * This method will be used to get the actual key array present in the
     * store.
     *
     * @param fileHolder file holder will be used to read the file
     * @return uncompressed keys
     * will return uncompressed key
     */
    @Override
    public byte[] getBackArray(FileHolder fileHolder) {
        return datastore;
    }

    /**
     * This method will be used to get the key array based on index
     *
     * @param index      index in store
     * @param fileHolder file holder will be used to read the file
     * @return key
     */
    @Override
    public byte[] get(int index, FileHolder fileHolder) {
        // create new array of size of each element
        byte[] copy = new byte[sizeOfEachElement];

        // copy array for given index
        // copy will done based on below calculation
        // eg: index is 4 and size of each key is 6 then copy from 6*4= 24th
        // index till 29th index
        System.arraycopy(datastore, ((index) * sizeOfEachElement), copy, 0, sizeOfEachElement);
        return copy;
    }

    /**
     * This method will clear the store and create the new empty store
     */
    @Override
    public void clear() {
        datastore = new byte[this.totalNumberOfElements * this.sizeOfEachElement];
    }
}
