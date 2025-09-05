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

import java.io.DataInput;
import java.io.IOException;
import java.io.Serializable;

public class DBFField implements Serializable {

    private static final int TERMINATOR = 0x0d;

    private String fieldName; /* 0-10  */
    private byte dataType; /* 11    */
    private int reserved1; /* 12-15 */
    private int fieldLength; /* 16    */
    private byte decimalCount; /* 17    */
    private short reserved2; /* 18-19 */
    private byte workAreaId; /* 20    */
    private short reserved3; /* 21-22 */
    private byte setFieldsFlag; /* 23    */
    private byte[] reserved4 = new byte[7]; /* 24-30 */
    private byte indexFieldFlag; /* 31    */
    private String actualType;

    private DBFField() {
    }

    public static DBFField read(final DataInput in) throws IOException {
        final DBFField field = new DBFField();

        final byte firstByte = in.readByte(); /* 0     */
        if (firstByte == TERMINATOR) {
            return null;
        }

        final byte[] bytes = new byte[11]; /* 1-10  */
        in.readFully(bytes, 1, 10);
        bytes[0] = firstByte;

        int nonZeroIndex = bytes.length - 1;
        while (nonZeroIndex >= 0 && bytes[nonZeroIndex] == 0) {
            nonZeroIndex--;
        }
        field.fieldName = new String(bytes, 0, nonZeroIndex + 1);

        field.dataType = in.readByte(); /* 11    */
        field.reserved1 = in.readInt();// DbfUtils.readLittleEndianInt(in);   /* 12-15 */
        field.fieldLength = in.readUnsignedByte(); /* 16    */
        field.decimalCount = in.readByte(); /* 17    */
        field.reserved2 = in.readShort(); // DbfUtils.readLittleEndianShort(in); /* 18-19 */
        field.workAreaId = in.readByte(); /* 20    */
        field.reserved3 = in.readShort();// DbfUtils.readLittleEndianShort(in); /* 21-22 */
        field.setFieldsFlag = in.readByte(); /* 23    */
        in.readFully(field.reserved4); /* 24-30 */
        field.indexFieldFlag = in.readByte(); /* 31    */
        /*
        Set actual type of the field here.
         */
        switch (field.dataType) {
            case 'C':
                field.actualType = "string";
                break;
            case 'D':
                field.actualType = "date";
                break;
            case 'F':
                field.actualType = "float";
                break;
            case 'L':
                field.actualType = "bool";
                break;
            case 'N':
                if (field.decimalCount == 0) {
                    if (field.fieldLength < 5)
                        field.actualType = "short";
                    else if (field.fieldLength < 8) {
                        field.actualType = "int";
                    } else
                        field.actualType = "long";
                } else {
                    field.actualType = "double";
                }
                break;
            default:
                field.actualType = null;
        }
        return field;
    }

    @Override
    public String toString() {
        final char c = (char) dataType;
        final StringBuilder sb = new StringBuilder("DBFField{");
        sb.append("fieldName='").append(fieldName).append('\'');
        sb.append(", dataType='").append(c).append('\'');
        sb.append(", fieldLength=").append(fieldLength);
        sb.append(", decimalCount=").append(decimalCount);
        sb.append('}');
        return sb.toString();
    }

    public int getFieldLength() {
        return fieldLength;
    }

    public String getActualType() {
        return actualType;
    }

    public String getFieldName() {
        return fieldName;
    }
}
