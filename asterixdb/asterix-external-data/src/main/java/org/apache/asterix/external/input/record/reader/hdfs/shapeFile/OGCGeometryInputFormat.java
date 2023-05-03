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
import java.util.List;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.external.api.IDataParser;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport.DBFField;
import org.apache.asterix.external.input.record.reader.hdfs.shapeFile.DBFReadSupport.DBFType;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMutableGeometry;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCMultiPoint;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;

public class OGCGeometryInputFormat extends AbstractShpInputFormat<VoidPointable> {
    private ARecordType recordType;
    /* list of fields requested to project in the SELECT statement */
    private String requestedFields;
    /* information about the MBR of the geometry on which the filter would be applied */
    private String filterMBRInfo;

    @Override
    public RecordReader<Void, VoidPointable> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter)
            throws IOException {
        try {
            return new ShapeFileReader(inputSplit, conf, reporter, recordType, requestedFields, filterMBRInfo);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void setRecordType(ARecordType type) {
        this.recordType = type;
    }

    public void setRequestedFields(String requestedFields) {
        this.requestedFields = requestedFields;
    }

    public void setFilterMBRInfo(String filterMBRInfo) {
        this.filterMBRInfo = filterMBRInfo;
    }
    /* */
    private static final class ShapeFileReader extends AbstractShapeReader<VoidPointable> {
        private final RecordBuilder builder;
        private final ARecordType recordType;

        public ShapeFileReader(InputSplit inputSplit, JobConf conf, Reporter reporter, ARecordType recordType,
                String requestedFields, String filterMBRInfo) throws IOException, InterruptedException {
            super(inputSplit, conf, reporter, requestedFields, filterMBRInfo);
            this.recordType = recordType;
            builder = new RecordBuilder();
        }
        @Override
        public boolean next(Void key, VoidPointable value) throws IOException {
            /* initialize a record builder which will be used to parse record of the shapefile in ADM format. */
            builder.init();
            /* set the record type of the builder to the defined record type */
            builder.reset(this.recordType);
            /*
            * if the query select count(*) only, we do not read the shp file, we can get the record counts from the index file(.shx)
            * index(.shx) file contains record offset for each record of the corresponding shape(.shp) file
            */
            if (readShxFile) {
                boolean hasMore = m_shxReader.hasMore();
                if (!hasMore)
                    return false;
                long recordOffset = m_shxReader.readRecord();
                ArrayBackedValueStorage nameBuffer = new ArrayBackedValueStorage();
                ArrayBackedValueStorage valueBytes = new ArrayBackedValueStorage();
                IDataParser.toBytes(new AMutableString("Record_Offset"), nameBuffer, stringSerde);
                aInt64.setValue(recordOffset);
                IDataParser.toBytes(aInt64, valueBytes, int64Serde);
                builder.addField(nameBuffer, valueBytes);
                ArrayBackedValueStorage valueContainer = new ArrayBackedValueStorage();
                builder.write(valueContainer.getDataOutput(), true);
                value.set(valueContainer);
                return true;
            }
            if (readGeometryField) {
                if (!m_shpReader.hasMore())
                    return false;
                int fieldIndex;
                /*
                * hasReadFully is a boolean flag to indicate whether the current geometry record of the .shp file has been
                * read or not.
                */
                boolean hasReadFully = true;
                OGCGeometry geometry = null;
                m_shpReader.readRecordHeader();
                switch (m_shpReader.getShapeType()) {
                    case 1:
                    case 11: //PointZ
                    case 21: //PointM
                        geometry = new OGCPoint(m_shpReader.readNewPoint(), SpatialReference.create(4326));
                        break;
                    case 3: //PolyLine
                    case 13:
                    case 23:
                        Polyline polyline = new Polyline();
                        hasReadFully = m_shpReader.readNewPolyline(polyline);
                        if (hasReadFully) {
                            if (m_shpReader.getNumParts() == 1) {
                                geometry = new OGCLineString(polyline, 0, SpatialReference.create(4326));
                            } else {
                                geometry = new OGCMultiLineString(polyline, SpatialReference.create(4326));
                            }
                        }
                        break;
                    case 8: //MultiPoint
                    case 18: //MultiPointZ
                    case 28: //MultiPointM
                        MultiPoint multiPoint = new MultiPoint();
                        hasReadFully = m_shpReader.readNewMultiPoint(multiPoint);
                        if (hasReadFully) {
                            geometry = new OGCMultiPoint(multiPoint, SpatialReference.create(4326));
                        }
                        break;
                    case 5:
                    case 15:
                    case 25:
                        Polygon p = new Polygon();
                        hasReadFully = m_shpReader.readNewPolygon(p);

                        if (hasReadFully) {
                            if (p.getExteriorRingCount() > 1) {
                                geometry = new OGCMultiPolygon(p, SpatialReference.create(4326));
                            } else
                                geometry = new OGCPolygon(p, SpatialReference.create(4326));
                        }
                        break;

                    default:
                        throw new IllegalStateException("Unexpected value: " + m_shpReader.getShapeType());
                }
                /*
                * if the record geometry is read fully, then we are going add the 'g' field and its value
                *  to the ADM record builder if not declared in the schema
                * if 'g' field is already defined in the schema, then we just need to get the field index from
                * the builder, and set the value
                 */
                if (hasReadFully) {
                    String fieldName = "g";
                    fieldIndex = recordType.getFieldIndex(fieldName);
                    ArrayBackedValueStorage valueBuffer = new ArrayBackedValueStorage();
                    ISerializerDeserializer<AMutableGeometry> gSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AGEOMETRY);
                    //aGeomtry.setValue(geometry);
                    AMutableGeometry aGeom = new AMutableGeometry(geometry);
                    IDataParser.toBytes(aGeom, valueBuffer, gSerde);

                    if (fieldIndex < 0) {
                        //field is not defined and the type is open
                        AMutableString str = new AMutableString("g");
                        ArrayBackedValueStorage nameBuffer = new ArrayBackedValueStorage();
                        IDataParser.toBytes(str, nameBuffer, stringSerde);
                        builder.addField(nameBuffer, valueBuffer);

                    } else {
                        final IAType fieldType = recordType.getFieldType(fieldName);
                        if (fieldType.getTypeTag() == aGeomtry.getType().getTypeTag()) {
                            builder.addField(fieldIndex, valueBuffer);
                        } else
                            throw new IllegalStateException("Defined type and Parsed Type do not match");
                    }
                }
                /*
                 * if geometry field is not read from the shapefile, the corresponding DBF record need to be skipped
                 * return an ADM record of BuiltInType.Anull type
                 */
                else {
                    if (readDBFFields)
                        m_dbfReader.skipBytes(m_dbfReader.getRecordLength());
                    ArrayBackedValueStorage valueContainer = new ArrayBackedValueStorage();
                    //set the record as NULL
                    IDataParser.toBytes(ANull.NULL, valueContainer, nullSerde);
                    value.set(valueContainer);
                    return true;
                }
            }
            //........ DBF File reading ............
            if (readDBFFields) {
                final byte dataType = m_dbfReader.nextDataType();
                if (dataType != DBFType.END) {
                    List<DBFField> fields = m_dbfReader.getFields();
                    for (DBFField field : fields) {
                        ArrayBackedValueStorage valueBytes = new ArrayBackedValueStorage();
                        final byte[] bytes = new byte[field.getFieldLength()];
                        m_dbfReader.readFully(bytes);
                        Object val;
                        IAType type;
                        switch (field.getActualType()) {
                            case "string":
                                val = new String(bytes).trim();
                                aString.setValue((String) val);
                                IDataParser.toBytes(aString, valueBytes, stringSerde);
                                type = BuiltinType.ASTRING;
                                break;

                            case "date":
                                val = readTimeInMillis(bytes);
                                aDate.setValue((Integer) val);
                                IDataParser.toBytes(aDate, valueBytes, dateSerde);
                                type = BuiltinType.ADATE;
                                break;

                            case "float":
                                val = readFloat(bytes);
                                aFloat.setValue((Float) val);
                                IDataParser.toBytes(aFloat, valueBytes, floatSerde);
                                type = BuiltinType.AFLOAT;
                                break;
                            case "bool":
                                val = readLogical(bytes);
                                IDataParser.toBytes((ABoolean) val, valueBytes, booleanSerde);
                                type = BuiltinType.ABOOLEAN;
                                break;

                            case "short":
                                val = readShort(bytes);
                                aInt16.setValue((Short) val);
                                IDataParser.toBytes(aInt16, valueBytes, int16Serde);
                                type = BuiltinType.AINT16;

                                break;
                            case "int":
                                val = readInteger(bytes);
                                aInt32.setValue((Integer) val);
                                IDataParser.toBytes(aInt32, valueBytes, int32Serde);
                                type = BuiltinType.AINT32;

                                break;
                            case "long":
                                val = readLong(bytes);
                                aInt64.setValue((Long) val);
                                IDataParser.toBytes(aInt64, valueBytes, int64Serde);
                                type = BuiltinType.AINT64;

                                break;
                            case "double":
                                val = readDouble(bytes);
                                aDouble.setValue((Double) val);
                                IDataParser.toBytes(aDouble, valueBytes, doubleSerde);
                                type = BuiltinType.ADOUBLE;
                                break;
                            default:
                                IDataParser.toBytes(ANull.NULL, valueBytes, nullSerde);
                                type = BuiltinType.ANULL;
                                break;
                        }
                        int fieldIndex = recordType.getFieldIndex(field.getFieldName());
                        if (fieldIndex < 0) {
                            //field is not defined and the type is open
                            AMutableString aString = new AMutableString(field.getFieldName());
                            ArrayBackedValueStorage nameBytes = new ArrayBackedValueStorage();
                            IDataParser.toBytes(aString, nameBytes, stringSerde);
                            builder.addField(nameBytes, valueBytes);

                        } else {
                            final IAType fieldType = recordType.getFieldType(field.getFieldName());
                            if (fieldType.getTypeTag() == type.getTypeTag() || type == BuiltinType.ANULL) {
                                builder.addField(fieldIndex, valueBytes);
                            } else
                                throw new IllegalStateException("Defined type and Parsed Type do not match");

                        }
                    }
                    //Sometimes DBF files contain padded dirty data at the end of actual record
                    //so need to check to skip those bytes
                    if (m_dbfReader.getRecordLength() > (m_dbfReader.getTotalFieldLength() + 1)) {
                        m_dbfReader.skipBytes(m_dbfReader.getRecordLength() - m_dbfReader.getTotalFieldLength() - 1);
                    }
                } else {
                    return false;
                }
            }
            /* Parse the record builder to VoidPointable object value if record builder successfully reach this point */
            ArrayBackedValueStorage valueContainer = new ArrayBackedValueStorage();
            builder.write(valueContainer.getDataOutput(), true);
            value.set(valueContainer);
            return true;
        }

        private int parseInt(final byte[] bytes, final int from, final int to) {
            int result = 0;
            for (int i = from; i < to && i < bytes.length; i++) {
                result *= 10;
                result += bytes[i] - '0';
            }
            return result;
        }

        private short parseShort(final byte[] bytes, final int from, final int to) {
            short result = 0;
            for (int i = from; i < to && i < bytes.length; i++) {
                result *= 10;
                result += bytes[i] - '0';
            }
            return result;
        }

        private long parseLong(final byte[] bytes, final int from, final int to) {
            long result = 0L;
            for (int i = from; i < to && i < bytes.length; i++) {
                result *= 10L;
                result += bytes[i] - '0';
            }
            return result;
        }

        private int trimSpaces(final byte[] bytes) {
            int i = 0, l = bytes.length;
            while (i < l) {
                if (bytes[i] != ' ') {
                    break;
                }
                i++;
            }
            return i;
        }

        private int readTimeInMillis(final byte[] bytes) throws IOException {
            int year = parseInt(bytes, 0, 4);
            int month = parseInt(bytes, 4, 6);
            int day = parseInt(bytes, 6, 8);
            long chronon = GregorianCalendarSystem.getInstance().getChronon(year, month, day, 0, 0, 0, 0);
            return GregorianCalendarSystem.getInstance().getChrononInDays(chronon);
        }

        private ABoolean readLogical(final byte[] bytes) throws IOException {
            switch (bytes[0]) {
                case 'Y':
                case 'y':
                case 'T':
                case 't':
                    return ABoolean.TRUE;
                default:
                    return ABoolean.FALSE;
            }

        }

        private short readShort(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0;
            }
            return parseShort(bytes, index, bytes.length);
        }

        private int readInteger(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0;
            }
            return parseInt(bytes, index, bytes.length);
        }

        private long readLong(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0L;
            }
            return parseLong(bytes, index, bytes.length);
        }

        private float readFloat(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0.0F;
            }
            // TODO - inline float reader
            return Float.parseFloat(new String(bytes, index, length));
        }

        private double readDouble(final byte[] bytes) throws IOException {
            final int index = trimSpaces(bytes);
            final int length = bytes.length - index;
            if (length == 0 || bytes[index] == '?') {
                return 0.0;
            }
            // TODO - inline double reader
            return Double.parseDouble(new String(bytes, index, length));
        }

        @Override
        public Void createKey() {
            return null;
        }

        @Override
        public VoidPointable createValue() {
            return new VoidPointable();
        }

        @Override
        public long getPos() throws IOException {
            return (long) getProgress();
        }
    }

}
