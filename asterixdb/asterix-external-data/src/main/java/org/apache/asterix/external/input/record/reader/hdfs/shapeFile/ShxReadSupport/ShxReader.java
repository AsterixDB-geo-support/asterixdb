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
package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShxReadSupport;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShpReadSupport.ShpHeader;

public class ShxReader implements Serializable {
    private transient DataInputStream dataInputStream;

    public transient ShpHeader shpHeader;
    public transient long recordOffset;
    public transient int recordLength;

    public ShxReader(final DataInputStream dataInputStream) throws IOException {
        this.dataInputStream = dataInputStream;
        shpHeader = new ShpHeader(dataInputStream);
    }

    public boolean hasMore() throws IOException {
        return dataInputStream.available() > 0;
    }

    /**
     * Read an SHX record.
     *
     * @return the seek position into the SHP file.
     * @throws IOException if an IO error occurs.
     */
    public long readRecord() throws IOException {
        recordOffset = dataInputStream.readInt();
        recordLength = dataInputStream.readInt();
        return recordOffset << 1;
    }
}
