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
package org.apache.asterix.external.input.record.reader.hdfs.shapeFile;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.ValueReferenceRecord;
import org.apache.asterix.external.input.record.reader.hdfs.AbstractHDFSRecordReader;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.AsterixParquetRuntimeException;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;

import java.io.IOException;

public class ShapeFileRecordReader<V extends IValueReference> extends AbstractHDFSRecordReader<Void, V> {
    private final IWarningCollector warningCollector;
    private final ARecordType recType;

    public ShapeFileRecordReader(boolean[] read, InputSplit[] inputSplits, String[] readSchedule, String nodeName,
            JobConf conf, IWarningCollector warningCollector, ARecordType recType, String requestedFields,
            String filterMBRInfo) {
        super(read, inputSplits, readSchedule, nodeName, new ValueReferenceRecord<>(), conf);
        this.warningCollector = warningCollector;
        this.recType = recType;
        if (inputFormat instanceof OGCGeometryInputFormat) {
            ((OGCGeometryInputFormat) inputFormat).setRecordType(recType);
            ((OGCGeometryInputFormat) inputFormat).setRequestedFields(requestedFields);
            ((OGCGeometryInputFormat) inputFormat).setFilterMBRInfo(filterMBRInfo);
        }
    }

    @Override
    protected boolean onNextInputSplit() throws IOException {
        return false;
    }

    @Override
    public IRawRecord<V> next() throws IOException {
        if (value instanceof VoidPointable) {
            //check if the read value is null or not. If ANull then it has only the typeTag in the serialized form
            if (value.getLength() <= 1)
                return null;
        }
        record.set(value);
        return record;
    }

    @Override
    public void close() throws IOException {
        super.close();
        //Issue warning if any was reported
        HDFSUtils.issueWarnings(warningCollector, conf);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RecordReader<Void, V> getRecordReader(int splitIndex) throws IOException {
        try {
            reader = (RecordReader<Void, V>) inputFormat.getRecordReader(inputSplits[splitIndex], conf, Reporter.NULL);
        } catch (AsterixParquetRuntimeException e) {
            throw e.getHyracksDataException();
        }
        if (value == null) {
            value = reader.createValue();
        }
        return reader;
    }
}
