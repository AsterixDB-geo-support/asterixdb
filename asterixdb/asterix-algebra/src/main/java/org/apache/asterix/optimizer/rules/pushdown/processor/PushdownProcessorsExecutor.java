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
package org.apache.asterix.optimizer.rules.pushdown.processor;

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.optimizer.rules.pushdown.PushdownContext;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.visitor.ExpectedSchemaNodeToIATypeTranslatorVisitor;
import org.apache.asterix.runtime.projection.ColumnDatasetProjectionFiltrationInfo;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.DefaultProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.metadata.IProjectionFiltrationInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;

public class PushdownProcessorsExecutor {
    private final List<IPushdownProcessor> processors;

    public PushdownProcessorsExecutor() {
        this.processors = new ArrayList<>();
    }

    public void add(IPushdownProcessor processor) {
        processors.add(processor);
    }

    public void execute() throws AlgebricksException {
        for (IPushdownProcessor processor : processors) {
            processor.process();
        }
    }

    public void finalizePushdown(PushdownContext pushdownContext, IOptimizationContext context) {
        for (ScanDefineDescriptor scanDefineDescriptor : pushdownContext.getRegisteredScans()) {
            Dataset dataset = scanDefineDescriptor.getDataset();
            AbstractScanOperator scanOp = (AbstractScanOperator) scanDefineDescriptor.getOperator();
            IProjectionFiltrationInfo info = null;
            if (dataset.getDatasetFormatInfo().getFormat() == DatasetConfig.DatasetFormat.COLUMN) {
                info = createInternalColumnarDatasetInfo(scanDefineDescriptor, context);
            } else if (dataset.getDatasetType() == DatasetConfig.DatasetType.EXTERNAL
                    && DatasetUtil.isFieldAccessPushdownSupported(dataset)) {
                info = createExternalDatasetProjectionInfo(scanDefineDescriptor, context);
            }
            setInfoToDataScan(scanOp, info);
        }
    }

    private IProjectionFiltrationInfo createInternalColumnarDatasetInfo(ScanDefineDescriptor scanDefineDescriptor,
            IOptimizationContext context) {
        Map<String, FunctionCallInformation> pathLocations = scanDefineDescriptor.getPathLocations();
        ARecordType recordRequestedType = ALL_FIELDS_TYPE;
        ARecordType metaRequestedType = scanDefineDescriptor.hasMeta() ? ALL_FIELDS_TYPE : null;

        // Pushdown field access only if it is enabled
        if (context.getPhysicalOptimizationConfig().isExternalFieldPushdown()) {
            ExpectedSchemaNodeToIATypeTranslatorVisitor converter =
                    new ExpectedSchemaNodeToIATypeTranslatorVisitor(pathLocations);
            recordRequestedType = (ARecordType) scanDefineDescriptor.getRecordNode().accept(converter,
                    scanDefineDescriptor.getDataset().getDatasetName());
            if (metaRequestedType != null) {
                metaRequestedType = (ARecordType) scanDefineDescriptor.getMetaNode().accept(converter,
                        scanDefineDescriptor.getDataset().getDatasetName());
            }
        }

        // Still allow for filter pushdowns even if value access pushdown is disabled
        return new ColumnDatasetProjectionFiltrationInfo(recordRequestedType, metaRequestedType, pathLocations,
                scanDefineDescriptor.getFilterPaths(), scanDefineDescriptor.getFilterExpression(),
                scanDefineDescriptor.getRangeFilterExpression());
    }

    private IProjectionFiltrationInfo createExternalDatasetProjectionInfo(ScanDefineDescriptor scanDefineDescriptor,
            IOptimizationContext context) {
        if (!context.getPhysicalOptimizationConfig().isExternalFieldPushdown()) {
            return DefaultProjectionFiltrationInfo.INSTANCE;
        }

        Map<String, FunctionCallInformation> pathLocations = scanDefineDescriptor.getPathLocations();
        ExpectedSchemaNodeToIATypeTranslatorVisitor converter =
                new ExpectedSchemaNodeToIATypeTranslatorVisitor(pathLocations);
        ARecordType recordRequestedType = (ARecordType) scanDefineDescriptor.getRecordNode().accept(converter,
                scanDefineDescriptor.getDataset().getDatasetName());
        return new ExternalDatasetProjectionInfo(recordRequestedType, pathLocations);
    }

    private void setInfoToDataScan(AbstractScanOperator scanOp, IProjectionFiltrationInfo info) {
        if (info == null) {
            return;
        }

        if (scanOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dataScanOp = (DataSourceScanOperator) scanOp;
            dataScanOp.setProjectionFiltrationInfo(info);
        } else if (scanOp.getOperatorTag() == LogicalOperatorTag.UNNEST_MAP) {
            UnnestMapOperator unnestMapOp = (UnnestMapOperator) scanOp;
            unnestMapOp.setProjectionFiltrationInfo(info);
        }
    }
}
