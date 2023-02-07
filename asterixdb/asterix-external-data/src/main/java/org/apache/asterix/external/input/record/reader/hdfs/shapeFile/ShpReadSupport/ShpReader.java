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
package org.apache.asterix.external.input.record.reader.hdfs.shapeFile.ShpReadSupport;

import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
 */
public class ShpReader implements Serializable {
    private transient DataInputStream m_dataInputStream;
    public transient ShpHeader m_shpHeader;

    private transient int m_parts[] = new int[4];

    private transient int recordNumber;
    private transient int contentLength;
    private transient int contentLengthInBytes;
    private transient int shapeType;
    protected transient double filterXmin;
    protected transient double filterYmin;
    protected transient double filterXmax;
    protected transient double filterYmax;
    private transient double mmin;
    private transient double mmax;
    private transient int numParts;
    private transient int numPoints;
    protected boolean isFilterMBRPushdown;

    public ShpReader(final DataInputStream dataInputStream, String filterMBRInfo) throws IOException {
        m_dataInputStream = dataInputStream;
        m_shpHeader = new ShpHeader(dataInputStream);
        if (filterMBRInfo != null) {
            isFilterMBRPushdown = true;
            String[] coordinates = filterMBRInfo.split(",");
            filterXmin = Double.parseDouble(coordinates[0]);
            filterYmin = Double.parseDouble(coordinates[1]);
            filterXmax = Double.parseDouble(coordinates[2]);
            filterYmax = Double.parseDouble(coordinates[3]);
            if (!m_shpHeader.isOverlapped(filterXmin, filterYmin, filterXmax, filterYmax)) {
                m_dataInputStream.skipBytes((m_shpHeader.fileLength * 2) - 100);
            }
        }
    }

    public ShpHeader getHeader() {
        return m_shpHeader;
    }

    public boolean hasMore() throws IOException {
        return m_dataInputStream.available() > 0;
    }

    public void readRecordHeader() throws IOException {
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4;
        //contentLengthInBytes = contentLength + contentLength;
        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
    }

    public Point readNewPoint() throws IOException {
        Point point = new Point();
        point.setX(EndianUtils.readSwappedDouble(m_dataInputStream));
        point.setY(EndianUtils.readSwappedDouble(m_dataInputStream));
        if (shapeType == 21) {
            point.setM(EndianUtils.readSwappedDouble(m_dataInputStream));
        } else if (shapeType == 11) {
            point.setZ(EndianUtils.readSwappedDouble(m_dataInputStream));
            point.setM(EndianUtils.readSwappedDouble(m_dataInputStream));

        }
        return point;
    }

    public boolean readNewPolygon(Polygon polygon) throws IOException {
        polygon.setEmpty();

        double xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return false;
            }
        }
        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        //int[] m_parts = new int[numParts+1];

        if ((numParts + 1) > m_parts.length) {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++) {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
        Point[] points = new Point[numPoints];
        for (int i = 0; i < numPoints; i++) {
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            points[i] = new Point(x, y);
        }
        if (shapeType == 15) {
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16 + 16 + numPoints * 8)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        if (shapeType == 25) { //MultiPointM
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        for (int i = 0; i < numParts; i++) {
            int startIndex = m_parts[i];
            int endIndex = m_parts[i + 1] - 1;
            polygon.startPath(points[startIndex]);
            for (int j = startIndex + 1; j <= endIndex; j++) {
                polygon.lineTo(points[j]);
            }
        }
        polygon.closeAllPaths();
        return true;
    }

    public boolean readNewPolyline(Polyline polyLine) throws IOException {
        polyLine.setEmpty();

        double xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return false;
            }
        }
        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        m_parts = new int[numParts + 1];

        if ((numParts + 1) > m_parts.length) {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++) {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
        Point[] points = new Point[numPoints];
        for (int i = 0; i < numPoints; i++) {
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            points[i] = new Point(x, y);
        }
        if (shapeType == 13) {
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16 + 16 + numPoints * 8)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        if (shapeType == 23) { //MultiPointM
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        for (int i = 0; i < numParts; i++) {
            int startIndex = m_parts[i];
            int endIndex = m_parts[i + 1] - 1;
            polyLine.startPath(points[startIndex]);
            for (int j = startIndex + 1; j <= endIndex; j++) {
                polyLine.lineTo(points[j]);
            }
        }
        polyLine.closeAllPaths();
        return true;
    }

    public boolean readNewMultiPoint(MultiPoint multiPoint) throws IOException {
        multiPoint.setEmpty();
        double xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return false;
            }
        }

        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        Point[] points = new Point[numPoints];
        double[] mValues = new double[numPoints];
        for (int i = 0; i < numPoints; i++) {
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            points[i] = new Point(x, y);
        }
        if (shapeType == 18) { //MultiPointZ
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            if (contentLengthInBytes > (36 + numPoints * 16 + 16 + numPoints * 8)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        if (shapeType == 28) { //MultiPointM
            if (contentLengthInBytes > (36 + numPoints * 16)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        for (int i = 0; i < numPoints; i++) {
            multiPoint.add(points[i]);
        }
        return true;
    }

    private boolean isOverlapped(double xmin, double ymin, double xmax, double ymax) {
        if (xmin > filterXmax || filterXmin > xmax || ymin > filterYmax || filterYmin > ymax)
            return false;
        return true;
    }

    public int getShapeType() {
        return shapeType;
    }

    public int getNumParts() {
        return numParts;
    }
}
