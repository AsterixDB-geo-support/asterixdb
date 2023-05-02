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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.io.EndianUtils;

import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;

/**
 * http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
 */
public class ShpReader implements Serializable {
    private transient DataInputStream m_dataInputStream;
    private transient ShpHeader m_shpHeader;
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
        m_shpHeader = new ShpHeader(m_dataInputStream);
        if (filterMBRInfo != null) {
            isFilterMBRPushdown = true;
            String[] coordinates = filterMBRInfo.split(",");
            filterXmin = Double.parseDouble(coordinates[0]);
            filterYmin = Double.parseDouble(coordinates[1]);
            filterXmax = Double.parseDouble(coordinates[2]);
            filterYmax = Double.parseDouble(coordinates[3]);
            // Check whether the query filter MBR overlaps the MBR of the whole file geometry.
            if (!m_shpHeader.isOverlapped(filterXmin, filterYmin, filterXmax, filterYmax)) {
                //skip the whole file
                m_dataInputStream.skipBytes((m_shpHeader.getFileLength() * 2) - 100);
            }
        }
    }

    public ShpHeader getHeader() {
        return m_shpHeader;
    }

    public boolean hasMore() throws IOException {
        //checked if any byte is available to read or not.
        return m_dataInputStream.read() != -1;
    }

    public void readRecordHeader() throws IOException {
        //First byte of the record header is already read during hasMore(), the remaining three bytes of record number
        //data is read in a dummy buffer.
        byte[] buffer = new byte[3];
        m_dataInputStream.readFully(buffer);
        contentLength = m_dataInputStream.readInt();
        //contentLength is stored in shapefile as 16-bit words. So need to multiply by two to get the length in byte
        //minus 4 because each record content starts with 4 byte shapeType. So actual data content is after this 4 bytes.
        contentLengthInBytes = contentLength + contentLength - 4;
        //contentLengthInBytes = contentLength + contentLength;
        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
    }

    public Point readNewPoint() throws IOException {
        Point point = new Point();
        point.setX(EndianUtils.readSwappedDouble(m_dataInputStream));
        point.setY(EndianUtils.readSwappedDouble(m_dataInputStream));
        //shape type: PointM
        if (shapeType == 21) {
            point.setM(EndianUtils.readSwappedDouble(m_dataInputStream));
        }
        //shape type: PointZ
        else if (shapeType == 11) {
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

        /*if minimum bounding rectangle of the record polygon does not overlap with the given filter MBR
        we are going to skip the entire record geometry*/
        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return false;
            }
        }
        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        int[] m_parts = new int[numParts + 1];
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
        //shape type: PolygonZ
        if (shapeType == 15) {
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            //The following part is optional in the record. So need to check whether the content contains more data or not.
            //we have contentLengthInBytes from record header, we use that to compare with the length of the bytes read so far.
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16 + 16 + numPoints * 8)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        //case: Polygon M
        if (shapeType == 25) {
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
        /*if minimum bounding rectangle of the record polyline does not overlap with the given filter MBR
        we are going to skip the entire record geometry*/
        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return false;
            }
        }
        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        int[] m_parts = new int[numParts + 1];
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
            //The following part is optional in the record. So need to check whether the content contains more data or not.
            //we have contentLengthInBytes from record header, we use that to compare with the length of the bytes read so far.
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16 + 16 + numPoints * 8)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    points[i].setM(m);
                }
            }
        }
        if (shapeType == 23) { //PolyLineM
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
        return true;
    }

    public boolean readNewMultiPoint(MultiPoint multiPoint) throws IOException {
        multiPoint.setEmpty();
        double xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        /*if minimum bounding rectangle of the record multipoint does not overlap with the given filter MBR
        we are going to skip the entire record geometry*/
        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return false;
            }
        }

        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        Point[] points = new Point[numPoints];
        for (int i = 0; i < numPoints; i++) {
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            points[i] = new Point(x, y);
        }
        //shape type: MultiPointZ
        if (shapeType == 18) {
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                points[i].setZ(z);
            }
            //The following part is optional in the record. So need to check whether the content contains more data or not.
            //we have contentLengthInBytes from record header, we use that to compare with the length of the bytes read so far.
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
            if (contentLengthInBytes > (36 + numPoints * 16)) { //the M measure can be optional
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
