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
package org.apache.asterix.column.operation.lsm.merge;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.column.tuple.MergeColumnTupleReference;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.column.values.IColumnValuesWriter;
import org.apache.asterix.column.values.writer.ColumnBatchWriter;
import org.apache.asterix.column.values.writer.filters.AbstractColumnFilterWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.AbstractColumnTupleWriter;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnTupleIterator;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnWriteMultiPageOp;

public class MergeColumnTupleWriter extends AbstractColumnTupleWriter {
    private final MergeColumnWriteMetadata columnMetadata;
    private final MergeColumnTupleReference[] componentsTuples;
    private final RunLengthIntArray writtenComponents;

    private final IColumnValuesWriter[] primaryKeyWriters;
    private final PriorityQueue<IColumnValuesWriter> orderedColumns;
    private final ColumnBatchWriter writer;
    private final int maxNumberOfTuples;
    private int primaryKeysEstimatedSize;

    public MergeColumnTupleWriter(MergeColumnWriteMetadata columnMetadata, int pageSize, int maxNumberOfTuples,
            double tolerance) {
        this.columnMetadata = columnMetadata;
        List<IColumnTupleIterator> componentsTuplesList = columnMetadata.getComponentsTuples();
        this.componentsTuples = new MergeColumnTupleReference[componentsTuplesList.size()];
        for (int i = 0; i < componentsTuplesList.size(); i++) {
            MergeColumnTupleReference mergeTuple = (MergeColumnTupleReference) componentsTuplesList.get(i);
            this.componentsTuples[i] = mergeTuple;
            mergeTuple.registerEndOfPageCallBack(this::writeAllColumns);
        }
        this.writtenComponents = new RunLengthIntArray();
        this.maxNumberOfTuples = maxNumberOfTuples;
        writer = new ColumnBatchWriter(columnMetadata.getMultiPageOpRef(), pageSize, tolerance);
        writtenComponents.reset();

        primaryKeyWriters = new IColumnValuesWriter[columnMetadata.getNumberOfPrimaryKeys()];
        for (int i = 0; i < primaryKeyWriters.length; i++) {
            primaryKeyWriters[i] = columnMetadata.getWriter(i);
        }
        orderedColumns = new PriorityQueue<>(Comparator.comparingInt(x -> -x.getEstimatedSize()));
    }

    @Override
    public int bytesRequired(ITupleReference tuple) {
        int primaryKeysSize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            primaryKeysSize += tuple.getFieldLength(i);
        }

        return primaryKeysSize;
    }

    @Override
    public void init(IColumnWriteMultiPageOp multiPageOp) throws HyracksDataException {
        columnMetadata.init(multiPageOp);
    }

    @Override
    public int getNumberOfColumns() {
        return columnMetadata.getNumberOfColumns();
    }

    @Override
    public int getMaxNumberOfTuples() {
        return maxNumberOfTuples;
    }

    @Override
    public int getOccupiedSpace() {
        int numberOfColumns = getNumberOfColumns();
        int filterSize = numberOfColumns * AbstractColumnFilterWriter.FILTER_SIZE;
        return primaryKeysEstimatedSize + filterSize;
    }

    @Override
    public void writeTuple(ITupleReference tuple) throws HyracksDataException {
        MergeColumnTupleReference columnTuple = (MergeColumnTupleReference) tuple;
        int componentIndex = columnTuple.getComponentIndex();
        int skipCount = columnTuple.getAndResetSkipCount();
        if (skipCount > 0) {
            writtenComponents.add(-componentIndex, skipCount);
        }
        if (columnTuple.isAntimatter()) {
            writtenComponents.add(-componentIndex);
        } else {
            writtenComponents.add(componentIndex);
        }
        writePrimaryKeys(columnTuple);
    }

    private void writePrimaryKeys(MergeColumnTupleReference columnTuple) throws HyracksDataException {
        int primaryKeySize = 0;
        for (int i = 0; i < columnMetadata.getNumberOfPrimaryKeys(); i++) {
            IColumnValuesReader columnReader = columnTuple.getReader(i);
            IColumnValuesWriter columnWriter = primaryKeyWriters[i];
            columnReader.write(columnWriter, false);
            primaryKeySize += columnWriter.getEstimatedSize();
        }
        primaryKeysEstimatedSize = primaryKeySize;
    }

    private void writeNonKeyColumns() throws HyracksDataException {
        for (int i = 0; i < writtenComponents.getNumberOfBlocks(); i++) {
            int componentIndex = writtenComponents.getBlockValue(i);
            if (componentIndex < 0) {
                //Skip writing values of deleted tuples
                componentIndex = -componentIndex;
                skipReaders(componentIndex, writtenComponents.getBlockSize(i));
                continue;
            }
            MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
            int count = writtenComponents.getBlockSize(i);
            for (int j = columnMetadata.getNumberOfPrimaryKeys(); j < columnMetadata.getNumberOfColumns(); j++) {
                IColumnValuesReader columnReader = componentTuple.getReader(j);
                IColumnValuesWriter columnWriter = columnMetadata.getWriter(j);
                columnReader.write(columnWriter, count);
            }
        }
    }

    private void skipReaders(int componentIndex, int count) throws HyracksDataException {
        MergeColumnTupleReference componentTuple = componentsTuples[componentIndex];
        for (int j = columnMetadata.getNumberOfPrimaryKeys(); j < columnMetadata.getNumberOfColumns(); j++) {
            IColumnValuesReader columnReader = componentTuple.getReader(j);
            columnReader.skip(count);
        }
    }

    @Override
    public int flush(ByteBuffer pageZero) throws HyracksDataException {
        int numberOfColumns = columnMetadata.getNumberOfColumns();
        int numberOfPrimaryKeys = columnMetadata.getNumberOfPrimaryKeys();
        if (writtenComponents.getSize() > 0) {
            writeNonKeyColumns();
            writtenComponents.reset();
        }
        for (int i = numberOfPrimaryKeys; i < numberOfColumns; i++) {
            orderedColumns.add(columnMetadata.getWriter(i));
        }
        writer.setPageZeroBuffer(pageZero, numberOfColumns, numberOfPrimaryKeys);
        int allocatedSpace = writer.writePrimaryKeyColumns(primaryKeyWriters);
        allocatedSpace += writer.writeColumns(orderedColumns);
        return allocatedSpace;
    }

    @Override
    public void close() {
        columnMetadata.close();
    }

    private void writeAllColumns(MergeColumnTupleReference columnTuple) throws HyracksDataException {
        /*
         * The last tuple from one of the components was reached. Since we are going to the next leaf, we will not be
         * able to access the readers of this component's leaf after this tuple. So, we are going to write
         * the values of all columns as recorded in writtenComponents
         */
        int skipCount = columnTuple.getAndResetSkipCount();
        if (skipCount > 0) {
            writtenComponents.add(-columnTuple.getComponentIndex(), skipCount);
        }
        writeNonKeyColumns();
        writtenComponents.reset();
    }
}
