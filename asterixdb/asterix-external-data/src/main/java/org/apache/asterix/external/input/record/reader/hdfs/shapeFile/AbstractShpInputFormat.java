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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.data.std.api.IValueReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
abstract class AbstractShpInputFormat<T extends IValueReference>
        extends org.apache.hadoop.mapred.FileInputFormat<Void, T> {
    @Override
    protected FileStatus[] listStatus(final JobConf job) throws IOException {
        final FileStatus[] orig = super.listStatus(job);
        final List<FileStatus> list = new ArrayList<FileStatus>(orig.length);
        for (final FileStatus fileStatus : orig) {
            final String name = fileStatus.getPath().getName().toLowerCase();
            if (name.endsWith(".shp")) {
                list.add(fileStatus);
            }
        }
        final FileStatus[] dest = new FileStatus[list.size()];
        list.toArray(dest);
        return dest;
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

}
