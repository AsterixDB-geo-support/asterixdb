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
package org.apache.hyracks.cloud.sweeper;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.cloud.cache.unit.SweepableIndexUnit;
import org.apache.hyracks.cloud.io.ICloudIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.cloud.IIndexDiskCacheManager;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class Sweeper {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final SweepRequest POISON = new SweepRequest();
    private final BlockingQueue<SweepRequest> requests;
    private final BlockingQueue<SweepRequest> freeRequests;
    private final AtomicBoolean shutdown;

    public Sweeper(ExecutorService executor, ICloudIOManager cloudIOManager, BufferCache bufferCache,
            Map<Integer, BufferedFileHandle> fileInfoMap, int numOfSweepThreads, int queueSize) {
        requests = new ArrayBlockingQueue<>(queueSize);
        freeRequests = new ArrayBlockingQueue<>(queueSize);
        shutdown = new AtomicBoolean(false);
        for (int i = 0; i < queueSize; i++) {
            freeRequests.add(new SweepRequest(new SweepContext(cloudIOManager, bufferCache, fileInfoMap, shutdown)));
        }
        for (int i = 0; i < numOfSweepThreads; i++) {
            executor.execute(new SweepThread(requests, freeRequests, i));
        }
    }

    public void sweep(SweepableIndexUnit indexUnit) throws InterruptedException {
        SweepRequest request = freeRequests.take();
        request.reset(indexUnit);
        requests.put(request);
    }

    public void shutdown() {
        shutdown.set(true);
        requests.clear();
        freeRequests.clear();
        requests.offer(POISON);
        // TODO wait for threads to terminate
    }

    private static class SweepThread implements Runnable {
        private final BlockingQueue<SweepRequest> requests;
        private final BlockingQueue<SweepRequest> freeRequests;
        private final int threadNumber;

        private SweepThread(BlockingQueue<SweepRequest> requests, BlockingQueue<SweepRequest> freeRequests,
                int threadNumber) {
            this.requests = requests;
            this.freeRequests = freeRequests;
            this.threadNumber = threadNumber;
        }

        @Override
        public void run() {
            Thread.currentThread().setName(getClass().getSimpleName() + "-" + threadNumber);
            while (true) {
                SweepRequest request = null;
                try {
                    request = requests.take();
                    if (isPoison(request)) {
                        break;
                    }
                    request.handle();
                } catch (InterruptedException e) {
                    LOGGER.warn("Ignoring interrupt. Sweep threads should never be interrupted.");
                } catch (Throwable t) {
                    LOGGER.error("Sweep failed", t);
                } finally {
                    if (request != null && request != POISON) {
                        freeRequests.add(request);
                    }
                }

            }
        }

        private boolean isPoison(SweepRequest request) {
            if (request == POISON) {
                LOGGER.info("Exiting");
                InvokeUtil.doUninterruptibly(() -> requests.put(POISON));
                if (Thread.interrupted()) {
                    LOGGER.error("Ignoring interrupt. Sweep threads should never be interrupted.");
                }
                return true;
            }

            return false;
        }
    }

    private static class SweepRequest {
        private final SweepContext context;

        SweepRequest() {
            this(null);
        }

        SweepRequest(SweepContext context) {
            this.context = context;
        }

        void reset(SweepableIndexUnit indexUnit) {
            context.setIndexUnit(indexUnit);
        }

        void handle() throws HyracksDataException {
            if (context.stopSweeping()) {
                /*
                 * This could happen as the sweeper gets a copy of a list of all indexUnits at a certain point
                 * of time. However, after acquiring the list and the start of handling this sweep request, we found
                 * that the index of this request was dropped.
                 *
                 * To illustrate:
                 * 1- The Sweeper got a list of all indexUnits (say index_1 and index_2)
                 * 2a- The Sweeper started sweeping index_1
                 * 2b- index_2 was dropped
                 * 3- The sweeper finished sweeping index_1 and started sweeping index_2. However, index_2 was
                 * dropped at 2b.
                 */
                return;
            }
            SweepableIndexUnit indexUnit = context.getIndexUnit();
            indexUnit.startSweeping();
            try {
                ILSMIndex index = indexUnit.getIndex();
                IIndexDiskCacheManager diskCacheManager = index.getDiskCacheManager();
                if (!diskCacheManager.isActive()) {
                    return;
                }

                if (diskCacheManager.prepareSweepPlan()) {
                    // The Index sweep planner determined that a sweep can be performed. Sweep.
                    diskCacheManager.sweep(context);
                }
            } finally {
                indexUnit.finishedSweeping();
            }
        }
    }
}
