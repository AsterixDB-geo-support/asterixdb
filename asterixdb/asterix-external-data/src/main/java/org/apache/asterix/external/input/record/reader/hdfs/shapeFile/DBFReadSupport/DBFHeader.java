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

import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DBFHeader implements Serializable {

    private byte signature; /* 0     */
    private byte year; /* 1     */
    private byte month; /* 2     */
    private byte day; /* 3     */
    private int numberOfRecords; /* 4-7   */
    private short headerLength; /* 8-9   */
    private short recordLength; /* 10-11 */
    private short reserved1; /* 12-13 */
    private byte incompleteTransaction; /* 14    */
    private byte encryptionFlag; /* 15    */
    private int freeRecordThread; /* 16-19 */
    private int reserved2; /* 20-23 */
    private int reserved3; /* 24-27 */
    private byte mdxFlag; /* 28    */
    private byte languageDriver; /* 29    */
    private short reserved4; /* 30-31 */
    private List<DBFField> fields; /* each 32 bytes */
    private final int numberOfFields;
    private int totalFieldLengthInBytes;
    public DBFHeader(final DataInputStream dataInput) throws IOException {
        this.signature = dataInput.readByte(); /* 0     */
        this.year = dataInput.readByte(); /* 1     */
        this.month = dataInput.readByte(); /* 2     */
        this.day = dataInput.readByte(); /* 3     */
        this.numberOfRecords = EndianUtils.readSwappedInteger(dataInput);  /* 4-7   */

        this.headerLength = EndianUtils.readSwappedShort(dataInput);   /* 8-9   */
        this.recordLength = EndianUtils.readSwappedShort(dataInput);  /* 10-11 */

        this.reserved1 = dataInput.readShort();     /* 12-13 */
        this.incompleteTransaction = dataInput.readByte(); /* 14    */
        this.encryptionFlag = dataInput.readByte(); /* 15    */
        this.freeRecordThread = dataInput.readInt(); /* 16-19 */
        this.reserved2 = dataInput.readInt(); /* 20-23 */
        this.reserved3 = dataInput.readInt(); /* 24-27 */
        this.mdxFlag = dataInput.readByte(); /* 28    */
        this.languageDriver = dataInput.readByte(); /* 29    */
        this.reserved4 = dataInput.readShort();       /* 30-31 */

        this.fields = new ArrayList<DBFField>();
        DBFField field;
        int totalFieldLegth = 0;
        while ((field = DBFField.read(dataInput)) != null) {
            fields.add(field);
            totalFieldLegth += field.getFieldLength();
        }
        this.numberOfFields = this.fields.size();
        this.totalFieldLengthInBytes = totalFieldLegth;
    }
    public DBFField getField(final int i) {
        return fields.get(i);
    }

    public int getNumberOfRecords() {
        return numberOfRecords;
    }

    public short getRecordLength() {
        return recordLength;
    }

    public int getNumberOfFields() {
        return numberOfFields;
    }

    public int getTotalFieldLengthInBytes() {
        return totalFieldLengthInBytes;
    }
    public List<DBFField> getFields(){
        return this.fields;
    }
}
