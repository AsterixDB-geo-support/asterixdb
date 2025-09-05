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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.EndianUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

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
    private final GeometryFactory geometryFactory;

    public ShpReader(final DataInputStream dataInputStream, String filterMBRInfo) throws IOException {
        if (dataInputStream == null) {
            throw new IllegalArgumentException("DataInputStream cannot be null");
        }
        m_dataInputStream = dataInputStream;
        m_shpHeader = new ShpHeader(m_dataInputStream);
        geometryFactory = new GeometryFactory();
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
        if (m_dataInputStream == null) {
            return false;
        }
        //checked if any byte is available to read or not.
        return m_dataInputStream.available() > 0;
    }

    public void readRecordHeader() throws IOException {
        if (m_dataInputStream == null) {
            throw new IOException("DataInputStream is null, cannot read record header");
        }
        //Read all 4 bytes of the record number (no bytes consumed by hasMore() anymore)
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        //contentLength is stored in shapefile as 16-bit words. So need to multiply by two to get the length in byte
        //minus 4 because each record content starts with 4 byte shapeType. So actual data content is after this 4 bytes.
        contentLengthInBytes = contentLength + contentLength - 4;
        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
    }

    public Point readNewPoint() throws IOException {
        if (m_dataInputStream == null) {
            throw new IOException("DataInputStream is null, cannot read point");
        }
        double x = EndianUtils.readSwappedDouble(m_dataInputStream);
        double y = EndianUtils.readSwappedDouble(m_dataInputStream);
        double z = Double.NaN; // Default to NaN for Z if not available
        double m = Double.NaN; // Default to NaN for M if not available

        // Shape type: PointM
        if (shapeType == 21) {
            m = EndianUtils.readSwappedDouble(m_dataInputStream);
        }
        // Shape type: PointZ
        else if (shapeType == 11) {
            z = EndianUtils.readSwappedDouble(m_dataInputStream);
            m = EndianUtils.readSwappedDouble(m_dataInputStream);
            Coordinate coordinate = new Coordinate(x, y, z);
            return geometryFactory.createPoint(coordinate);
        }
        Coordinate coordinate = new Coordinate(x, y);
        return geometryFactory.createPoint(coordinate);
    }

    public MultiPolygon readNewPolygon() throws IOException {
        if (m_dataInputStream == null) {
            throw new IOException("DataInputStream is null, cannot read polygon");
        }
        double xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        /*if minimum bounding rectangle of the record polygon does not overlap with the given filter MBR
        we are going to skip the entire record geometry*/
        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return null;
            }
        }
        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);

        if (numParts <= 0) {
            throw new IOException("Invalid number of parts: " + numParts);
        }
        if (numPoints <= 0) {
            throw new IOException("Invalid number of points: " + numPoints);
        }

        int[] m_parts = new int[numParts + 1];
        for (int p = 0; p < numParts; p++) {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;

        Coordinate[] coordinates = new Coordinate[numPoints];
        for (int i = 0; i < numPoints; i++) {
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            coordinates[i] = new Coordinate(x, y);
        }
        //shape type: PolygonZ
        if (shapeType == 15) {
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                coordinates[i].setZ(z);
            }
            //The following part is optional in the record. So need to check whether the content contains more data or not.
            //we have contentLengthInBytes from record header, we use that to compare with the length of the bytes read so far.
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16 + 16 + numPoints * 8)) {
                //TODO(suryaa) - Why are these there? Ask tomal?
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    //coordinates[i].setM(m);
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
                    //coordinates[i].setM(m);
                }
            }
        }
        List<Polygon> polygons = new ArrayList<>();

        for (int i = 0; i < numParts; i++) {
            int startIndex = m_parts[i];
            int endIndex = m_parts[i + 1] - 1;
            Coordinate[] partCoordinates = new Coordinate[endIndex - startIndex + 1];
            System.arraycopy(coordinates, startIndex, partCoordinates, 0, partCoordinates.length);

            if (!partCoordinates[0].equals2D(partCoordinates[partCoordinates.length - 1])) {
                partCoordinates = closeRing(partCoordinates);
            }

            LinearRing ring = geometryFactory.createLinearRing(partCoordinates);

            // Validate the ring before using it
            if (!ring.isValid()) {
                throw new IOException("Invalid linear ring detected in polygon at part " + i
                        + ". Ring may be self-intersecting or malformed.");
            }

            if (i == 0) {
                // First part is the outer shell of a Polygon
                Polygon newPolygon = geometryFactory.createPolygon(ring, null);
                if (!newPolygon.isValid()) {
                    throw new IOException("Invalid polygon created from ring " + i
                            + ". Polygon may be self-intersecting or malformed.");
                }
                polygons.add(newPolygon);
            } else {
                // Subsequent parts can either be holes for the first Polygon or new Polygons
                Polygon lastPolygon = polygons.get(polygons.size() - 1);
                if (lastPolygon.getNumInteriorRing() == 0) {
                    // If the last Polygon has no holes, add this part as a hole
                    Polygon polygonWithHole =
                            geometryFactory.createPolygon(lastPolygon.getExteriorRing(), new LinearRing[] { ring });
                    if (!polygonWithHole.isValid()) {
                        throw new IOException("Invalid polygon with hole created at part " + i
                                + ". Polygon may be self-intersecting or malformed.");
                    }
                    polygons.set(polygons.size() - 1, polygonWithHole);
                } else {
                    // Otherwise, start a new Polygon
                    Polygon newPolygon = geometryFactory.createPolygon(ring, null);
                    if (!newPolygon.isValid()) {
                        throw new IOException("Invalid polygon created from ring " + i
                                + ". Polygon may be self-intersecting or malformed.");
                    }
                    polygons.add(newPolygon);
                }
            }
        }
        MultiPolygon multiPolygon = geometryFactory.createMultiPolygon(polygons.toArray(new Polygon[0]));
        if (!multiPolygon.isValid()) {
            throw new IOException(
                    "Invalid MultiPolygon created. The geometry may contain self-intersections or other topological errors.");
        }
        return multiPolygon;
    }

    private Coordinate[] closeRing(Coordinate[] coordinates) {
        Coordinate[] closedCoordinates = new Coordinate[coordinates.length + 1];
        System.arraycopy(coordinates, 0, closedCoordinates, 0, coordinates.length);
        closedCoordinates[coordinates.length] = coordinates[0]; // Close the ring
        return closedCoordinates;
    }

    public MultiLineString readNewPolyline() throws IOException {
        if (m_dataInputStream == null) {
            throw new IOException("DataInputStream is null, cannot read polyline");
        }
        double xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymax = EndianUtils.readSwappedDouble(m_dataInputStream);
        /* if minimum bounding rectangle of the record polyline does not overlap with the given filter MBR
        we are going to skip the entire record geometry */
        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return null;
            }
        }
        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        int[] m_parts = new int[numParts + 1];
        for (int p = 0; p < numParts; p++) {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
        Coordinate[] coordinates = new Coordinate[numPoints];
        for (int i = 0; i < numPoints; i++) {
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            coordinates[i] = new Coordinate(x, y);
        }
        if (shapeType == 13) {
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                coordinates[i].setZ(z);
            }
            //The following part is optional in the record. So need to check whether the content contains more data or not.
            //we have contentLengthInBytes from record header, we use that to compare with the length of the bytes read so far.
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16 + 16 + numPoints * 8)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    //M dimension is not fully supported at the time of this commit by JTS
                    //coordinates[i].setM(m);
                }
            }
        }
        if (shapeType == 23) { //PolyLineM
            if (contentLengthInBytes > (40 + numParts * 4 + numPoints * 16)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    //coordinates[i].setM(m);
                }
            }
        }
        // Create LineStrings for each part and add them to the MultiLineString
        LineString[] lineStrings = new LineString[numParts];
        for (int i = 0; i < numParts; i++) {
            int startIndex = m_parts[i];
            int endIndex = m_parts[i + 1];
            Coordinate[] partCoordinates = new Coordinate[endIndex - startIndex];
            System.arraycopy(coordinates, startIndex, partCoordinates, 0, partCoordinates.length);
            LineString lineString = geometryFactory.createLineString(partCoordinates);

            // Validate the LineString
            if (!lineString.isValid()) {
                throw new IOException("Invalid LineString created at part " + i
                        + ". LineString may contain duplicate consecutive points or other issues.");
            }
            lineStrings[i] = lineString;
        }

        // Create the MultiLineString from the LineStrings
        MultiLineString multiLineString = geometryFactory.createMultiLineString(lineStrings);
        if (!multiLineString.isValid()) {
            throw new IOException("Invalid MultiLineString created. The geometry may contain invalid line segments.");
        }
        return multiLineString;
    }

    public MultiPoint readNewMultiPoint() throws IOException {
        if (m_dataInputStream == null) {
            throw new IOException("DataInputStream is null, cannot read multipoint");
        }
        double xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        double xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        double ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        /* if minimum bounding rectangle of the record multipoint does not overlap with the given filter MBR
        we are going to skip the entire record geometry */
        if (isFilterMBRPushdown) {
            if (!isOverlapped(xmin, ymin, xmax, ymax)) {
                m_dataInputStream.skipBytes(contentLengthInBytes - 32);
                return null;
            }
        }

        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);
        Coordinate[] coordinates = new Coordinate[numPoints];
        for (int i = 0; i < numPoints; i++) {
            final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
            final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
            coordinates[i] = new Coordinate(x, y);
        }
        //shape type: MultiPointZ
        if (shapeType == 18) {
            double zMin = EndianUtils.readSwappedDouble(m_dataInputStream);
            double zMax = EndianUtils.readSwappedDouble(m_dataInputStream);
            for (int i = 0; i < numPoints; i++) {
                final double z = EndianUtils.readSwappedDouble(m_dataInputStream);
                coordinates[i].setZ(z);
            }
            //The following part is optional in the record. So need to check whether the content contains more data or not.
            //we have contentLengthInBytes from record header, we use that to compare with the length of the bytes read so far.
            if (contentLengthInBytes > (36 + numPoints * 16 + 16 + numPoints * 8)) {
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    //coordinates[i].setM(m);
                }
            }
        }
        if (shapeType == 28) { //MultiPointM
            if (contentLengthInBytes > (36 + numPoints * 16)) { //the M measure can be optional
                double mMin = EndianUtils.readSwappedDouble(m_dataInputStream);
                double mMax = EndianUtils.readSwappedDouble(m_dataInputStream);
                for (int i = 0; i < numPoints; i++) {
                    final double m = EndianUtils.readSwappedDouble(m_dataInputStream);
                    //coordinates[i].setM(m);
                }
            }
        }
        MultiPoint multiPoint = geometryFactory.createMultiPointFromCoords(coordinates);
        if (!multiPoint.isValid()) {
            throw new IOException("Invalid MultiPoint created. The geometry may contain invalid coordinates.");
        }
        return multiPoint;
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
