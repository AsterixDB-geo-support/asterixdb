/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ILSMComponentFinalizer {
    
    /**
     * @return Whether the given LSM component is considered valid. Used for guaranteeing
     *         atomicity of LSM component writes.
     */
    public boolean isValid(Object lsmComponent) throws HyracksDataException;
    
    /**
     * Marks the given LSM component as physically valid, synchronously forcing
     * the necessary information to disk. This call only return once the
     * physical consistency of the given component is guaranteed.
     * 
     */
    public void finalize(Object lsmComponent) throws HyracksDataException;
}
