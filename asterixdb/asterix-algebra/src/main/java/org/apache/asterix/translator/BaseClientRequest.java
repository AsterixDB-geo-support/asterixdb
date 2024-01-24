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
package org.apache.asterix.translator;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.api.RequestReference;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.om.base.ADateTime;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class BaseClientRequest implements IClientRequest {

    private boolean complete;
    private final IRequestReference requestReference;
    private boolean cancellable = false;
    private volatile long completionTime = -1;
    protected volatile State state = State.RECEIVED;

    public BaseClientRequest(IRequestReference requestReference) {
        this.requestReference = requestReference;
    }

    @Override
    public synchronized void complete() {
        if (complete) {
            return;
        }
        complete = true;
        state = State.COMPLETED;
        completionTime = System.currentTimeMillis();
    }

    @Override
    public synchronized void cancel(ICcApplicationContext appCtx) throws HyracksDataException {
        if (complete) {
            return;
        }
        complete();
        state = State.CANCELLED;
        if (cancellable) {
            doCancel(appCtx);
        }
    }

    @Override
    public synchronized void markCancellable() {
        cancellable = true;
    }

    @Override
    public synchronized boolean isCancelled() {
        return state == State.CANCELLED;
    }

    @Override
    public String getId() {
        // the uuid is generated by the node which received the request
        // so there is a chance this might not be unique now
        return requestReference.getUuid();
    }

    @Override
    public synchronized boolean isCancellable() {
        return cancellable;
    }

    public void setRunning() {
        state = State.RUNNING;
    }

    @Override
    public String toJson() {
        return JSONUtil.convertNodeUnchecked(asJson());
    }

    @Override
    public ObjectNode asJson() {
        ObjectNode json = JSONUtil.createObject();
        json.put("uuid", requestReference.getUuid());
        json.put("requestTime", new ADateTime(requestReference.getTime()).toSimpleString());
        json.put("elapsedTime", getElapsedTimeInSecs());
        json.put("node", requestReference.getNode());
        json.put("state", state.getLabel());
        json.put("userAgent", ((RequestReference) requestReference).getUserAgent());
        json.put("remoteAddr", ((RequestReference) requestReference).getRemoteAddr());
        json.put("cancellable", cancellable);
        return json;
    }

    private double getElapsedTimeInSecs() {
        // this is just an estimation as the request might have been received on a node with a different system time
        long runningTime = completionTime > 0 ? completionTime : System.currentTimeMillis();
        return (runningTime - requestReference.getTime()) / 1000d;
    }

    protected abstract void doCancel(ICcApplicationContext appCtx) throws HyracksDataException;
}
