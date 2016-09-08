/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.config;

import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.StorageUtil;

import static org.apache.hyracks.util.StorageUtil.StorageUnit.KILOBYTE;
import static org.apache.hyracks.util.StorageUtil.StorageUnit.MEGABYTE;

public class AsterixStorageProperties extends AbstractAsterixProperties {

    private static final String STORAGE_BUFFERCACHE_PAGESIZE_KEY = "storage.buffercache.pagesize";
    private static final int STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT = StorageUtil.getSizeInBytes(128, KILOBYTE);

    private static final String STORAGE_BUFFERCACHE_SIZE_KEY = "storage.buffercache.size";
    private static final long STORAGE_BUFFERCACHE_SIZE_DEFAULT = StorageUtil.getSizeInBytes(512, MEGABYTE);

    private static final String STORAGE_BUFFERCACHE_MAXOPENFILES_KEY = "storage.buffercache.maxopenfiles";
    private static final int STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT = Integer.MAX_VALUE;

    private static final String STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY = "storage.memorycomponent.pagesize";
    private static final int STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT = StorageUtil.getSizeInBytes(128, KILOBYTE);

    private static final String STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY = "storage.memorycomponent.numpages";
    private static final int STORAGE_MEMORYCOMPONENT_NUMPAGES_DEFAULT = 256; // ... so 32MB components

    private static final String STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY =
            "storage.metadata.memorycomponent.numpages";
    private static final int STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_DEFAULT = 256; // ... so 32MB components

    private static final String STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY = "storage.memorycomponent.numcomponents";
    private static final int STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_DEFAULT = 2; // 2 components

    private static final String STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY = "storage.memorycomponent.globalbudget";
    private static final long STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT = StorageUtil.getSizeInBytes(512, MEGABYTE);

    private static final String STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY =
            "storage.lsm.bloomfilter.falsepositiverate";
    private static final double STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT = 0.01;

    public AsterixStorageProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    @PropertyKey(STORAGE_BUFFERCACHE_PAGESIZE_KEY)
    public int getBufferCachePageSize() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_PAGESIZE_KEY, STORAGE_BUFFERCACHE_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    @PropertyKey(STORAGE_BUFFERCACHE_SIZE_KEY)
    public long getBufferCacheSize() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_SIZE_KEY, STORAGE_BUFFERCACHE_SIZE_DEFAULT,
                PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    public int getBufferCacheNumPages() {
        return (int) (getBufferCacheSize() / (getBufferCachePageSize() + IBufferCache.RESERVED_HEADER_BYTES));
    }

    @PropertyKey(STORAGE_BUFFERCACHE_MAXOPENFILES_KEY)
    public int getBufferCacheMaxOpenFiles() {
        return accessor.getProperty(STORAGE_BUFFERCACHE_MAXOPENFILES_KEY, STORAGE_BUFFERCACHE_MAXOPENFILES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY)
    public int getMemoryComponentPageSize() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_PAGESIZE_KEY, STORAGE_MEMORYCOMPONENT_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerBytePropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY)
    public int getMemoryComponentNumPages() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_NUMPAGES_KEY, STORAGE_MEMORYCOMPONENT_NUMPAGES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY)
    public int getMetadataMemoryComponentNumPages() {
        return accessor.getProperty(STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_KEY,
                        STORAGE_METADATA_MEMORYCOMPONENT_NUMPAGES_DEFAULT,
                        PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY)
    public int getMemoryComponentsNum() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_KEY,
                STORAGE_MEMORYCOMPONENT_NUMCOMPONENTS_DEFAULT, PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    @PropertyKey(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY)
    public long getMemoryComponentGlobalBudget() {
        return accessor.getProperty(STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_KEY,
                STORAGE_MEMORYCOMPONENT_GLOBALBUDGET_DEFAULT, PropertyInterpreters.getLongBytePropertyInterpreter());
    }

    @PropertyKey(STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY)
    public double getBloomFilterFalsePositiveRate() {
        return accessor.getProperty(STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_KEY,
                STORAGE_LSM_BLOOMFILTER_FALSEPOSITIVERATE_DEFAULT, PropertyInterpreters.getDoublePropertyInterpreter());
    }
}
