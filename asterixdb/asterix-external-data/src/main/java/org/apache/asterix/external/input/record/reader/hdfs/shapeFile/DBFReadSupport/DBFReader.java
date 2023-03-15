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
package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport;

import org.apache.hadoop.io.Writable;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Based on https://code.google.com/p/javadbf/
 */
public class DBFReader implements Serializable {
    private final transient DataInputStream m_dataInputStream;
    private final transient DBFHeader m_header;

    public DBFReader(final DataInputStream dataInputStream) throws IOException {
        m_dataInputStream = dataInputStream;
        m_header = new DBFHeader(dataInputStream);
    }

    public List<DBFField> getFields() {
        return m_header.getFields();
    }

    public byte nextDataType() throws IOException {
        byte dataType;
        //read the first byte of DBF record content
        // if it is DBFType.DELETED, skip record, and then start reading the next record of the file
        //if the read byte is DBFType.END, that means end of the file reached.
        do {
            dataType = m_dataInputStream.readByte();
            if (dataType == DBFType.END) {
                break;
            } else if (dataType == DBFType.DELETED) {
                skipRecordExcludingFirstByte();
            }
        } while (dataType == DBFType.DELETED);
        return dataType;
    }

    public void skipRecordExcludingFirstByte() throws IOException {
        m_dataInputStream.skipBytes(m_header.getRecordLength() - 1);
    }

    public int getTotalFieldLength() {
        return m_header.getTotalFieldLengthInBytes();
    }

    public int getRecordLength() {
        return m_header.getRecordLength();
    }

    public void skipBytes(int n) throws IOException {
        m_dataInputStream.skipBytes(n);
    }

    public void readFully(byte[] bytes) throws IOException {
        m_dataInputStream.readFully(bytes);
    }

    public boolean hasMore() throws IOException {
        return m_dataInputStream.available() > 0;
    }
}
