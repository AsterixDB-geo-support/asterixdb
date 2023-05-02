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

import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport.DBFReader;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShpReadSupport.ShpReader;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShxReadSupport.ShxReader;
import org.apache.asterix.external.parser.AbstractDataParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.data.std.api.IValueReference;

public abstract class AbstractShapeReader<T extends IValueReference> extends AbstractDataParser
        implements RecordReader<Void, T> {
    protected long m_length;
    protected FSDataInputStream m_shpStream;
    protected ShpReader m_shpReader;
    protected FSDataInputStream m_dfbStream;
    protected FSDataInputStream m_shxStream;
    protected DBFReader m_dbfReader;
    protected ShxReader m_shxReader;
    protected boolean readGeometryField;
    protected boolean readDBFFields;
    protected boolean readShxFile = false;

    public AbstractShapeReader(InputSplit inputSplit, JobConf conf, Reporter reporter, String requestedFields,
            String filterMBRInfo) throws IOException {
        //
        if (inputSplit instanceof FileSplit) {
            if (requestedFields == null || requestedFields.equals("")) {
                readGeometryField = true;
                readDBFFields = true;
            } else if (requestedFields.equals("{}")) {
                // readNumberOfRecordsFromHeaderOnly = true;
                readShxFile = true;
                readGeometryField = false;
                readDBFFields = false;
            } else {
                String[] fields = requestedFields.split(",");
                if (requestedFields.isEmpty()) {
                    readGeometryField = true;
                    readDBFFields = true;
                } else {
                    readGeometryField = Arrays.asList(fields).contains("g");
                    if (readGeometryField && fields.length > 1) {
                        readDBFFields = true;
                    } else
                        readDBFFields = !readGeometryField;
                }
            }
            final FileSplit fileSplit = (FileSplit) inputSplit;
            m_length = fileSplit.getLength();
            final Path path = fileSplit.getPath();
            String shapePath = path.toString();
            final FileSystem fileSystem = FileSystem.get(conf);
            if (readGeometryField) {
                m_shpStream = fileSystem.open(path);
                m_shpReader = new ShpReader(m_shpStream, filterMBRInfo);
            }
            if (readDBFFields) {
                String dbfPath = shapePath.substring(0, shapePath.lastIndexOf('.')) + ".dbf";
                m_dfbStream = fileSystem.open(new Path(dbfPath));
                m_dbfReader = new DBFReader(m_dfbStream);
            }
            if (readShxFile) {
                String shxPath = shapePath.substring(0, shapePath.lastIndexOf('.')) + ".shx";
                m_shxStream = fileSystem.open(new Path(shxPath));
                m_shxReader = new ShxReader(m_shxStream);
            }

        } else {
            throw new IOException("Input split is not an instance of FileSplit");
        }

    }

    @Override
    public float getProgress() throws IOException {
        return m_length;
    }

    @Override
    public void close() throws IOException {
        if (m_shpStream != null) {
            m_shpStream.close();
            m_shpStream = null;
        }
        if (m_dfbStream != null) {
            m_dfbStream.close();
            m_dfbStream = null;
        }
        if (m_shxStream != null) {
            m_shxStream.close();
            m_shxStream = null;
        }
    }
}
