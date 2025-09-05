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
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.PRJReadSupport.PrjReader;
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
    protected PrjReader m_prjReader;
    protected FSDataInputStream m_dfbStream;
    protected FSDataInputStream m_shxStream;
    protected DBFReader m_dbfReader;
    protected ShxReader m_shxReader;
    protected boolean readGeometryField;
    protected boolean readDBFFields;
    protected boolean readShxFile = false;

    public AbstractShapeReader(InputSplit inputSplit, JobConf conf, Reporter reporter, String requestedFields,
            String filterMBRInfo) throws IOException {
        if (inputSplit instanceof FileSplit) {
            /*
            * if requested fields contains "" or null, that means we need to read both .shp and .dbf files
            * if requested fields contains {}, that means count(*) has been requested, in this case reading .shx file would be sufficient
            * if requested fields contains 'g' only, then we only need to read .shp file
            * other cases: read both .shp and .dbf files
            */
            if (requestedFields == null || requestedFields.equals("")) {
                readGeometryField = true;
                readDBFFields = true;
            } else if (requestedFields.equals("{}")) {
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
                if (!fileSystem.exists(path)) {
                    throw new IOException("Shapefile does not exist: " + path);
                }
                m_shpStream = fileSystem.open(path);
                m_shpReader = new ShpReader(m_shpStream, filterMBRInfo);

                // Open separate .prj file if it exists (don't use the same stream as .shp!)
                String prjPath = shapePath.substring(0, shapePath.lastIndexOf('.')) + ".prj";
                Path prjPathObj = new Path(prjPath);
                if (fileSystem.exists(prjPathObj)) {
                    try (FSDataInputStream prjStream = fileSystem.open(prjPathObj)) {
                        m_prjReader = new PrjReader(prjStream);
                    } catch (IOException e) {
                        // PRJ file exists but can't be read - log but continue
                        // PRJ file is optional, so this is acceptable
                    }
                }
            }
            if (readDBFFields) {
                String dbfPath = shapePath.substring(0, shapePath.lastIndexOf('.')) + ".dbf";
                Path dbfPathObj = new Path(dbfPath);
                if (!fileSystem.exists(dbfPathObj)) {
                    throw new IOException("Required DBF file does not exist: " + dbfPath);
                }
                m_dfbStream = fileSystem.open(dbfPathObj);
                m_dbfReader = new DBFReader(m_dfbStream);
            }
            if (readShxFile) {
                String shxPath = shapePath.substring(0, shapePath.lastIndexOf('.')) + ".shx";
                Path shxPathObj = new Path(shxPath);
                if (!fileSystem.exists(shxPathObj)) {
                    throw new IOException("Required SHX file does not exist: " + shxPath);
                }
                m_shxStream = fileSystem.open(shxPathObj);
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
