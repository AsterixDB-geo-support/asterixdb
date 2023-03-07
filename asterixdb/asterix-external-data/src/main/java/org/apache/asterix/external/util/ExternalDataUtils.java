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
package org.apache.asterix.external.util;

import static org.apache.asterix.external.util.ExternalDataConstants.KEY_DELIMITER;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_ESCAPE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_EXCLUDE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_EXTERNAL_SCAN_BUFFER_SIZE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_INCLUDE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_QUOTE;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_RECORD_END;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_RECORD_START;
import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.validateAzureBlobProperties;
import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.validateAzureDataLakeProperties;
import static org.apache.asterix.external.util.google.gcs.GCSUtils.validateProperties;
import static org.apache.asterix.runtime.evaluators.functions.StringEvaluatorUtils.RESERVED_REGEX_CHARS;
import static org.msgpack.core.MessagePack.Code.ARRAY16;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.library.ILibrary;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IDataParserFactory;
import org.apache.asterix.external.api.IExternalDataSourceFactory.DataSourceType;
import org.apache.asterix.external.api.IInputStreamFactory;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory.IncludeExcludeMatcher;
import org.apache.asterix.external.library.JavaLibrary;
import org.apache.asterix.external.library.msgpack.MessagePackUtils;
import org.apache.asterix.external.util.ExternalDataConstants.ParquetOptions;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.asterix.runtime.projection.DataProjectionInfo;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.parsers.BooleanParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.LongParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.util.StorageUtil;

public class ExternalDataUtils {
    private static final Map<ATypeTag, IValueParserFactory> valueParserFactoryMap = new EnumMap<>(ATypeTag.class);
    private static final int DEFAULT_MAX_ARGUMENT_SZ = 1024 * 1024;
    private static final int HEADER_FUDGE = 64;

    static {
        valueParserFactoryMap.put(ATypeTag.INTEGER, IntegerParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.FLOAT, FloatParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.DOUBLE, DoubleParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.BIGINT, LongParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.STRING, UTF8StringParserFactory.INSTANCE);
        valueParserFactoryMap.put(ATypeTag.BOOLEAN, BooleanParserFactory.INSTANCE);
    }

    private ExternalDataUtils() {
    }

    public static int getOrDefaultBufferSize(Map<String, String> configuration) {
        String bufferSize = configuration.get(KEY_EXTERNAL_SCAN_BUFFER_SIZE);
        return bufferSize != null ? Integer.parseInt(bufferSize) : ExternalDataConstants.DEFAULT_BUFFER_SIZE;
    }

    // Get a delimiter from the given configuration
    public static char validateGetDelimiter(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_DELIMITER, ExternalDataConstants.DEFAULT_DELIMITER.charAt(0));
    }

    // Get a quote from the given configuration when the delimiter is given
    // Need to pass delimiter to check whether they share the same character
    public static char validateGetQuote(Map<String, String> configuration, char delimiter) throws HyracksDataException {
        char quote = validateCharOrDefault(configuration, KEY_QUOTE, ExternalDataConstants.DEFAULT_QUOTE.charAt(0));
        validateDelimiterAndQuote(delimiter, quote);
        return quote;
    }

    public static char validateGetEscape(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_ESCAPE, ExternalDataConstants.ESCAPE);
    }

    public static char validateGetRecordStart(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_RECORD_START, ExternalDataConstants.DEFAULT_RECORD_START);
    }

    public static char validateGetRecordEnd(Map<String, String> configuration) throws HyracksDataException {
        return validateCharOrDefault(configuration, KEY_RECORD_END, ExternalDataConstants.DEFAULT_RECORD_END);
    }

    public static void validateDataParserParameters(Map<String, String> configuration) throws AsterixException {
        String parser = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (parser == null) {
            String parserFactory = configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
            if (parserFactory == null) {
                throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED,
                        ExternalDataConstants.KEY_FORMAT + " or " + ExternalDataConstants.KEY_PARSER_FACTORY);
            }
        }
    }

    public static void validateDataSourceParameters(Map<String, String> configuration) throws AsterixException {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if (reader == null) {
            throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_READER);
        }
    }

    public static DataSourceType getDataSourceType(Map<String, String> configuration) {
        String reader = configuration.get(ExternalDataConstants.KEY_READER);
        if ((reader != null) && reader.equals(ExternalDataConstants.READER_STREAM)) {
            return DataSourceType.STREAM;
        } else {
            return DataSourceType.RECORDS;
        }
    }

    public static boolean isExternal(String aString) {
        return ((aString != null) && aString.contains(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR)
                && (aString.trim().length() > 1));
    }

    public static String getLibraryName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[0];
    }

    public static String getExternalClassName(String aString) {
        return aString.trim().split(FeedConstants.NamingConstants.LIBRARY_NAME_SEPARATOR)[1];
    }

    public static IInputStreamFactory createExternalInputStreamFactory(ILibraryManager libraryManager,
            DataverseName dataverse, String stream) throws HyracksDataException {
        try {
            String libraryName = getLibraryName(stream);
            String className = getExternalClassName(stream);
            ILibrary lib = libraryManager.getLibrary(dataverse, libraryName);
            if (lib.getLanguage() != ExternalFunctionLanguage.JAVA) {
                throw new HyracksDataException("Unexpected library language: " + lib.getLanguage());
            }
            ClassLoader classLoader = ((JavaLibrary) lib).getClassLoader();
            return ((IInputStreamFactory) (classLoader.loadClass(className).newInstance()));
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeDataException(ErrorCode.UTIL_EXTERNAL_DATA_UTILS_FAIL_CREATE_STREAM_FACTORY, e);
        }
    }

    public static DataverseName getDatasetDataverse(Map<String, String> configuration) throws AsterixException {
        return DataverseName.createFromCanonicalForm(configuration.get(ExternalDataConstants.KEY_DATASET_DATAVERSE));
    }

    public static String getParserFactory(Map<String, String> configuration) {
        String parserFactory = configuration.get(ExternalDataConstants.KEY_PARSER);
        if (parserFactory != null) {
            return parserFactory;
        }
        parserFactory = configuration.get(ExternalDataConstants.KEY_FORMAT);
        return parserFactory != null ? parserFactory : configuration.get(ExternalDataConstants.KEY_PARSER_FACTORY);
    }

    public static IValueParserFactory[] getValueParserFactories(ARecordType recordType) {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = null;
            if (recordType.getFieldTypes()[i].getTypeTag() == ATypeTag.UNION) {
                AUnionType unionType = (AUnionType) recordType.getFieldTypes()[i];
                if (!unionType.isUnknownableType()) {
                    throw new NotImplementedException("Non-optional UNION type is not supported.");
                }
                tag = unionType.getActualType().getTypeTag();
            } else {
                tag = recordType.getFieldTypes()[i].getTypeTag();
            }
            if (tag == null) {
                throw new NotImplementedException("Failed to get the type information for field " + i + ".");
            }
            fieldParserFactories[i] = getParserFactory(tag);
        }
        return fieldParserFactories;
    }

    public static IValueParserFactory getParserFactory(ATypeTag tag) {
        IValueParserFactory vpf = valueParserFactoryMap.get(tag);
        if (vpf == null) {
            throw new NotImplementedException("No value parser factory for fields of type " + tag);
        }
        return vpf;
    }

    public static boolean hasHeader(Map<String, String> configuration) {
        return isTrue(configuration, ExternalDataConstants.KEY_HEADER);
    }

    public static boolean isTrue(Map<String, String> configuration, String key) {
        String value = configuration.get(key);
        return value == null ? false : Boolean.valueOf(value);
    }

    // Currently not used.
    public static IRecordReaderFactory<?> createExternalRecordReaderFactory(ILibraryManager libraryManager,
            Map<String, String> configuration) throws AsterixException {
        String readerFactory = configuration.get(ExternalDataConstants.KEY_READER_FACTORY);
        if (readerFactory == null) {
            throw new AsterixException("to use " + ExternalDataConstants.EXTERNAL + " reader, the parameter "
                    + ExternalDataConstants.KEY_READER_FACTORY + " must be specified.");
        }
        String[] libraryAndFactory = readerFactory.split(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        if (libraryAndFactory.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }
        String[] dataverseAndLibrary = libraryAndFactory[0].split("\\.");
        if (dataverseAndLibrary.length != 2) {
            throw new AsterixException("The parameter " + ExternalDataConstants.KEY_READER_FACTORY
                    + " must follow the format \"DataverseName.LibraryName#ReaderFactoryFullyQualifiedName\"");
        }
        DataverseName dataverseName = DataverseName.createSinglePartName(dataverseAndLibrary[0]); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        String libraryName = dataverseAndLibrary[1];
        ILibrary lib;
        try {
            lib = libraryManager.getLibrary(dataverseName, libraryName);
        } catch (HyracksDataException e) {
            throw new AsterixException("Cannot load library", e);
        }
        if (lib.getLanguage() != ExternalFunctionLanguage.JAVA) {
            throw new AsterixException("Unexpected library language: " + lib.getLanguage());
        }
        ClassLoader classLoader = ((JavaLibrary) lib).getClassLoader();
        try {
            return (IRecordReaderFactory<?>) classLoader.loadClass(libraryAndFactory[1]).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AsterixException("Failed to create record reader factory", e);
        }
    }

    // Currently not used.
    public static IDataParserFactory createExternalParserFactory(ILibraryManager libraryManager,
            DataverseName dataverse, String parserFactoryName) throws AsterixException {
        try {
            String library = parserFactoryName.substring(0,
                    parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR));
            ILibrary lib;
            try {
                lib = libraryManager.getLibrary(dataverse, library);
            } catch (HyracksDataException e) {
                throw new AsterixException("Cannot load library", e);
            }
            if (lib.getLanguage() != ExternalFunctionLanguage.JAVA) {
                throw new AsterixException("Unexpected library language: " + lib.getLanguage());
            }
            ClassLoader classLoader = ((JavaLibrary) lib).getClassLoader();
            return (IDataParserFactory) classLoader
                    .loadClass(parserFactoryName
                            .substring(parserFactoryName.indexOf(ExternalDataConstants.EXTERNAL_LIBRARY_SEPARATOR) + 1))
                    .newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new AsterixException("Failed to create an external parser factory", e);
        }
    }

    public static boolean isFeed(Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_IS_FEED)) {
            return false;
        } else {
            return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_FEED));
        }
    }

    public static void prepareFeed(Map<String, String> configuration, DataverseName dataverseName, String feedName) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_IS_FEED)) {
            configuration.put(ExternalDataConstants.KEY_IS_FEED, ExternalDataConstants.TRUE);
        }
        configuration.put(ExternalDataConstants.KEY_DATASET_DATAVERSE, dataverseName.getCanonicalForm());
        configuration.put(ExternalDataConstants.KEY_FEED_NAME, feedName);
    }

    public static boolean keepDataSourceOpen(Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_WAIT_FOR_DATA)) {
            return true;
        }
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_WAIT_FOR_DATA));
    }

    public static String getFeedName(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_FEED_NAME);
    }

    public static boolean isRecordWithMeta(Map<String, String> configuration) {
        return configuration.containsKey(ExternalDataConstants.KEY_META_TYPE_NAME);
    }

    public static void setRecordWithMeta(Map<String, String> configuration, String booleanString) {
        configuration.put(ExternalDataConstants.FORMAT_RECORD_WITH_METADATA, booleanString);
    }

    public static boolean isChangeFeed(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_CHANGE_FEED));
    }

    public static boolean isInsertFeed(Map<String, String> configuration) {
        return Boolean.parseBoolean(configuration.get(ExternalDataConstants.KEY_IS_INSERT_FEED));
    }

    public static int getNumberOfKeys(Map<String, String> configuration) throws AsterixException {
        String keyIndexes = configuration.get(ExternalDataConstants.KEY_KEY_INDEXES);
        if (keyIndexes == null) {
            throw AsterixException.create(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_KEY_INDEXES);
        }
        return keyIndexes.split(",").length;
    }

    public static void setNumberOfKeys(Map<String, String> configuration, int value) {
        configuration.put(ExternalDataConstants.KEY_KEY_SIZE, String.valueOf(value));
    }

    public static void setChangeFeed(Map<String, String> configuration, String booleanString) {
        configuration.put(ExternalDataConstants.KEY_IS_CHANGE_FEED, booleanString);
    }

    public static int[] getPKIndexes(Map<String, String> configuration) {
        String keyIndexes = configuration.get(ExternalDataConstants.KEY_KEY_INDEXES);
        String[] stringIndexes = keyIndexes.split(",");
        int[] intIndexes = new int[stringIndexes.length];
        for (int i = 0; i < stringIndexes.length; i++) {
            intIndexes[i] = Integer.parseInt(stringIndexes[i]);
        }
        return intIndexes;
    }

    public static int[] getPKSourceIndicators(Map<String, String> configuration) {
        String keyIndicators = configuration.get(ExternalDataConstants.KEY_KEY_INDICATORS);
        String[] stringIndicators = keyIndicators.split(",");
        int[] intIndicators = new int[stringIndicators.length];
        for (int i = 0; i < stringIndicators.length; i++) {
            intIndicators[i] = Integer.parseInt(stringIndicators[i]);
        }
        return intIndicators;
    }

    /**
     * Fills the configuration of the external dataset and its adapter with default values if not provided by user.
     *
     * @param configuration
     *            external data configuration
     */
    public static void defaultConfiguration(Map<String, String> configuration) {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (format != null) {
            // default quote, escape character for quote and fields delimiter for csv and tsv format
            if (format.equals(ExternalDataConstants.FORMAT_CSV)) {
                configuration.putIfAbsent(KEY_DELIMITER, ExternalDataConstants.DEFAULT_DELIMITER);
                configuration.putIfAbsent(KEY_QUOTE, ExternalDataConstants.DEFAULT_QUOTE);
                configuration.putIfAbsent(KEY_ESCAPE, ExternalDataConstants.DEFAULT_QUOTE);
            } else if (format.equals(ExternalDataConstants.FORMAT_TSV)) {
                configuration.putIfAbsent(KEY_DELIMITER, ExternalDataConstants.TAB_STR);
                configuration.putIfAbsent(KEY_QUOTE, ExternalDataConstants.NULL_STR);
                configuration.putIfAbsent(KEY_ESCAPE, ExternalDataConstants.NULL_STR);
            }
        }
    }

    /**
     * Prepares the configuration of the external data and its adapter by filling the information required by
     * adapters and parsers.
     *
     * @param adapterName
     *            adapter name
     * @param configuration
     *            external data configuration
     */
    public static void prepare(String adapterName, Map<String, String> configuration) {
        if (!configuration.containsKey(ExternalDataConstants.KEY_READER)) {
            configuration.put(ExternalDataConstants.KEY_READER, adapterName);
        }
        final String inputFormat = configuration.get(ExternalDataConstants.KEY_INPUT_FORMAT);
        if (ExternalDataConstants.INPUT_FORMAT_PARQUET.equals(inputFormat)) {
            //Parquet supports binary-to-binary conversion. No parsing is required
            configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_NOOP);
            configuration.put(ExternalDataConstants.KEY_FORMAT, ExternalDataConstants.FORMAT_PARQUET);
        }
        if(ExternalDataConstants.INPUT_FORMAT_SHAPE.equals(inputFormat)) {
            configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_NOOP);
            configuration.put(ExternalDataConstants.KEY_FORMAT, ExternalDataConstants.FORMAT_SHAPE);
        }
        if (!configuration.containsKey(ExternalDataConstants.KEY_PARSER)
                && configuration.containsKey(ExternalDataConstants.KEY_FORMAT)) {
            configuration.put(ExternalDataConstants.KEY_PARSER, configuration.get(ExternalDataConstants.KEY_FORMAT));
        }
    }

    /**
     * Normalizes the values of certain parameters of the adapter configuration. This should happen before persisting
     * the metadata (e.g. when creating external datasets or feeds) and when creating an adapter factory.
     *
     * @param configuration
     *            external data configuration
     */
    public static void normalize(Map<String, String> configuration) {
        // normalize the "format" parameter
        String paramValue = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (paramValue != null) {
            String lowerCaseFormat = paramValue.toLowerCase().trim();
            if (ExternalDataConstants.ALL_FORMATS.contains(lowerCaseFormat)) {
                configuration.put(ExternalDataConstants.KEY_FORMAT, lowerCaseFormat);
            }
        }
        // normalize "header" parameter
        putToLowerIfExists(configuration, ExternalDataConstants.KEY_HEADER);
        // normalize "redact-warnings" parameter
        putToLowerIfExists(configuration, ExternalDataConstants.KEY_REDACT_WARNINGS);
    }

    /**
     * Validates the parameter values of the adapter configuration. This should happen after normalizing the values.
     *
     * @param configuration
     *            external data configuration
     * @throws HyracksDataException
     *             HyracksDataException
     */
    public static void validate(Map<String, String> configuration) throws HyracksDataException {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        String header = configuration.get(ExternalDataConstants.KEY_HEADER);
        if (format != null && isHeaderRequiredFor(format) && header == null) {
            throw new RuntimeDataException(ErrorCode.PARAMETERS_REQUIRED, ExternalDataConstants.KEY_HEADER);
        }
        if (header != null && !isBoolean(header)) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, ExternalDataConstants.KEY_HEADER, header);
        }
        char delimiter = validateGetDelimiter(configuration);
        validateGetQuote(configuration, delimiter);
        validateGetEscape(configuration);
        String value = configuration.get(ExternalDataConstants.KEY_REDACT_WARNINGS);
        if (value != null && !isBoolean(value)) {
            throw new RuntimeDataException(ErrorCode.INVALID_REQ_PARAM_VAL, ExternalDataConstants.KEY_REDACT_WARNINGS,
                    value);
        }
    }

    private static boolean isHeaderRequiredFor(String format) {
        return format.equals(ExternalDataConstants.FORMAT_CSV) || format.equals(ExternalDataConstants.FORMAT_TSV);
    }

    private static boolean isBoolean(String value) {
        return value.equals(ExternalDataConstants.TRUE) || value.equals(ExternalDataConstants.FALSE);
    }

    private static void validateDelimiterAndQuote(char delimiter, char quote) throws RuntimeDataException {
        if (quote == delimiter) {
            throw new RuntimeDataException(ErrorCode.QUOTE_DELIMITER_MISMATCH, quote, delimiter);
        }
    }

    private static char validateCharOrDefault(Map<String, String> configuration, String key, char defaultValue)
            throws HyracksDataException {
        String value = configuration.get(key);
        if (value == null) {
            return defaultValue;
        }
        validateChar(value, key);
        return value.charAt(0);
    }

    public static void validateChar(String parameterValue, String parameterName) throws RuntimeDataException {
        if (parameterValue.length() != 1) {
            throw new RuntimeDataException(ErrorCode.INVALID_CHAR_LENGTH, parameterValue, parameterName);
        }
    }

    private static void putToLowerIfExists(Map<String, String> configuration, String key) {
        String paramValue = configuration.get(key);
        if (paramValue != null) {
            configuration.put(key, paramValue.toLowerCase().trim());
        }
    }

    /**
     * Validates adapter specific external dataset properties. Specific properties for different adapters should be
     * validated here
     *
     * @param configuration
     *            properties
     */
    public static void validateAdapterSpecificProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector, IApplicationContext appCtx) throws CompilationException {
        String type = configuration.get(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE);

        switch (type) {
            case ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3:
                S3Utils.validateProperties(configuration, srcLoc, collector);
                break;
            case ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_BLOB:
                validateAzureBlobProperties(configuration, srcLoc, collector, appCtx);
                break;
            case ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_DATA_LAKE:
                validateAzureDataLakeProperties(configuration, srcLoc, collector, appCtx);
                break;
            case ExternalDataConstants.KEY_ADAPTER_NAME_GCS:
                validateProperties(configuration, srcLoc, collector);
                break;
            default:
                // Nothing needs to be done
                break;
        }
    }

    /**
     * Regex matches all the provided patterns against the provided path
     *
     * @param path
     *            path to check against
     * @return {@code true} if all patterns match, {@code false} otherwise
     */
    public static boolean matchPatterns(List<Matcher> matchers, String path) {
        for (Matcher matcher : matchers) {
            if (matcher.reset(path).matches()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts the wildcard to proper regex
     *
     * @param pattern
     *            wildcard pattern to convert
     * @return regex expression
     */
    public static String patternToRegex(String pattern) {
        int charPosition = 0;
        int patternLength = pattern.length();
        StringBuilder stuffBuilder = new StringBuilder();
        StringBuilder result = new StringBuilder();

        while (charPosition < patternLength) {
            char c = pattern.charAt(charPosition);
            charPosition++;

            switch (c) {
                case '*':
                    result.append(".*");
                    break;
                case '?':
                    result.append(".");
                    break;
                case '[':
                    int closingBracketPosition = charPosition;
                    if (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) == '!') {
                        closingBracketPosition++;
                    }

                    // 2 cases can happen here:
                    // 1- Empty character class [] which is invalid for java, so treat ] as literal and find another
                    // closing bracket, if no closing bracket is found, the whole thing is a literal
                    // 2- Negated empty class [!] converted to [^] which is invalid for java, so treat ] as literal and
                    // find another closing bracket, if no closing bracket is found, the whole thing is a literal
                    if (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) == ']') {
                        closingBracketPosition++;
                    }

                    // No [] and [!] cases, search for the closing bracket
                    while (closingBracketPosition < patternLength && pattern.charAt(closingBracketPosition) != ']') {
                        closingBracketPosition++;
                    }

                    // No closing bracket found (or [] or [!]), escape the opening bracket, treat it as literals
                    if (closingBracketPosition >= patternLength) {
                        result.append("\\[");
                    } else {
                        // Found closing bracket, get the stuff in between the found the character class ("[" and "]")
                        String stuff = pattern.substring(charPosition, closingBracketPosition);

                        stuffBuilder.setLength(0);
                        int stuffCharPos = 0;

                        // If first character in the character class is "!" then convert it to "^"
                        if (stuff.charAt(0) == '!') {
                            stuffBuilder.append('^');
                            stuffCharPos++; // ignore first character when escaping metacharacters next step
                        }

                        for (; stuffCharPos < stuff.length(); stuffCharPos++) {
                            char stuffChar = stuff.charAt(stuffCharPos);
                            if (stuffChar != '-' && Arrays.binarySearch(RESERVED_REGEX_CHARS, stuffChar) >= 0) {
                                stuffBuilder.append("\\");
                            }
                            stuffBuilder.append(stuffChar);
                        }

                        String stuffEscaped = stuffBuilder.toString();

                        // Escape the set operations
                        stuffEscaped = stuffEscaped.replace("&&", "\\&\\&").replace("~~", "\\~\\~")
                                .replace("||", "\\|\\|").replace("--", "\\-\\-");

                        result.append("[").append(stuffEscaped).append("]");
                        charPosition = closingBracketPosition + 1;
                    }
                    break;
                default:
                    if (Arrays.binarySearch(RESERVED_REGEX_CHARS, c) >= 0) {
                        result.append("\\");
                    }
                    result.append(c);
                    break;
            }
        }

        return result.toString();
    }

    /**
     * Adjusts the prefix (if needed) and returns it
     *
     * @param configuration
     *            configuration
     */
    public static String getPrefix(Map<String, String> configuration) {
        return getPrefix(configuration, true);
    }

    public static String getPrefix(Map<String, String> configuration, boolean appendSlash) {
        String definition = configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
        String subPath = configuration.get(ExternalDataConstants.SUBPATH);
        boolean hasDefinition = definition != null && !definition.isEmpty();
        boolean hasSubPath = subPath != null && !subPath.isEmpty();
        if (hasDefinition && !hasSubPath) {
            return appendSlash ? definition + (!definition.endsWith("/") ? "/" : "") : definition;
        }
        String fullPath = "";
        if (hasSubPath) {
            if (!hasDefinition) {
                fullPath = subPath.startsWith("/") ? subPath.substring(1) : subPath;
            } else {
                // concatenate definition + subPath:
                if (definition.endsWith("/") && subPath.startsWith("/")) {
                    subPath = subPath.substring(1);
                } else if (!definition.endsWith("/") && !subPath.startsWith("/")) {
                    definition = definition + "/";
                }
                fullPath = definition + subPath;
            }
            fullPath = appendSlash ? fullPath + (!fullPath.endsWith("/") ? "/" : "") : fullPath;
        }
        return fullPath;
    }

    /**
     * @param configuration
     *            configuration map
     * @throws CompilationException
     *             Compilation exception
     */
    public static void validateIncludeExclude(Map<String, String> configuration) throws CompilationException {
        // Ensure that include and exclude are not provided at the same time + ensure valid format or property
        List<Map.Entry<String, String>> includes = new ArrayList<>();
        List<Map.Entry<String, String>> excludes = new ArrayList<>();

        // Accepted formats are include, include#1, include#2, ... etc, same for excludes
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            String key = entry.getKey();

            if (key.equals(ExternalDataConstants.KEY_INCLUDE)) {
                includes.add(entry);
            } else if (key.equals(ExternalDataConstants.KEY_EXCLUDE)) {
                excludes.add(entry);
            } else if (key.startsWith(ExternalDataConstants.KEY_INCLUDE)
                    || key.startsWith(ExternalDataConstants.KEY_EXCLUDE)) {

                // Split by the "#", length should be 2, left should be include/exclude, right should be integer
                String[] splits = key.split("#");

                if (key.startsWith(ExternalDataConstants.KEY_INCLUDE) && splits.length == 2
                        && splits[0].equals(ExternalDataConstants.KEY_INCLUDE)
                        && NumberUtils.isIntegerNumericString(splits[1])) {
                    includes.add(entry);
                } else if (key.startsWith(ExternalDataConstants.KEY_EXCLUDE) && splits.length == 2
                        && splits[0].equals(ExternalDataConstants.KEY_EXCLUDE)
                        && NumberUtils.isIntegerNumericString(splits[1])) {
                    excludes.add(entry);
                } else {
                    throw new CompilationException(ErrorCode.INVALID_PROPERTY_FORMAT, key);
                }
            }
        }

        // Ensure either include or exclude are provided, but not both of them
        if (!includes.isEmpty() && !excludes.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME,
                    ExternalDataConstants.KEY_INCLUDE, ExternalDataConstants.KEY_EXCLUDE);
        }
    }

    public static IncludeExcludeMatcher getIncludeExcludeMatchers(Map<String, String> configuration)
            throws CompilationException {
        // Get and compile the patterns for include/exclude if provided
        List<Matcher> includeMatchers = new ArrayList<>();
        List<Matcher> excludeMatchers = new ArrayList<>();
        String pattern = null;
        try {
            for (Map.Entry<String, String> entry : configuration.entrySet()) {
                if (entry.getKey().startsWith(KEY_INCLUDE)) {
                    pattern = entry.getValue();
                    includeMatchers.add(Pattern.compile(patternToRegex(pattern)).matcher(""));
                } else if (entry.getKey().startsWith(KEY_EXCLUDE)) {
                    pattern = entry.getValue();
                    excludeMatchers.add(Pattern.compile(patternToRegex(pattern)).matcher(""));
                }
            }
        } catch (PatternSyntaxException ex) {
            throw new CompilationException(ErrorCode.INVALID_REGEX_PATTERN, pattern);
        }

        IncludeExcludeMatcher includeExcludeMatcher;
        if (!includeMatchers.isEmpty()) {
            includeExcludeMatcher =
                    new IncludeExcludeMatcher(includeMatchers, (matchers1, key) -> matchPatterns(matchers1, key));
        } else if (!excludeMatchers.isEmpty()) {
            includeExcludeMatcher =
                    new IncludeExcludeMatcher(excludeMatchers, (matchers1, key) -> !matchPatterns(matchers1, key));
        } else {
            includeExcludeMatcher = new IncludeExcludeMatcher(Collections.emptyList(), (matchers1, key) -> true);
        }

        return includeExcludeMatcher;
    }

    public static boolean supportsPushdown(Map<String, String> properties) {
        //Currently, only Apache Parquet format is supported
        return isParquetFormat(properties) || isShapefileFormat(properties);
    }

    /**
     * Validate Parquet dataset's declared type and configuration
     *
     * @param properties
     *            external dataset configuration
     * @param datasetRecordType
     *            dataset declared type
     */
    public static void validateParquetTypeAndConfiguration(Map<String, String> properties,
            ARecordType datasetRecordType) throws CompilationException {
        if (isParquetFormat(properties)) {
            if (datasetRecordType.getFieldTypes().length != 0) {
                throw new CompilationException(ErrorCode.UNSUPPORTED_TYPE_FOR_PARQUET, datasetRecordType.getTypeName());
            } else if (properties.containsKey(ParquetOptions.TIMEZONE)
                    && !ParquetOptions.VALID_TIME_ZONES.contains(properties.get(ParquetOptions.TIMEZONE))) {
                //Ensure the configured time zone id is correct
                throw new CompilationException(ErrorCode.INVALID_TIMEZONE, properties.get(ParquetOptions.TIMEZONE));
            }
        }
    }

    public static boolean isParquetFormat(Map<String, String> properties) {
        String inputFormat = properties.get(ExternalDataConstants.KEY_INPUT_FORMAT);
        return ExternalDataConstants.CLASS_NAME_PARQUET_INPUT_FORMAT.equals(inputFormat)
                || ExternalDataConstants.INPUT_FORMAT_PARQUET.equals(inputFormat)
                || ExternalDataConstants.FORMAT_PARQUET.equals(properties.get(ExternalDataConstants.KEY_FORMAT));
    }

    public static boolean isShapefileFormat(Map<String, String> properties) {
        String inputFormat = properties.get(ExternalDataConstants.KEY_INPUT_FORMAT);
        return ExternalDataConstants.CLASS_NAME_SHP_INPUT_FORMAT.equals(inputFormat)
                || ExternalDataConstants.INPUT_FORMAT_SHAPE.equals(inputFormat);
    }
    public static void setExternalDataProjectionInfo(DataProjectionInfo projectionInfo, Map<String, String> properties)
            throws IOException {
        if(properties.get(ExternalDataConstants.KEY_INPUT_FORMAT).equals(ExternalDataConstants.INPUT_FORMAT_SHAPE)){
            String[] fields = projectionInfo.getProjectionInfo().getFieldNames();
            properties.put(ExternalDataConstants.KEY_REQUESTED_FIELDS, String.join(",", fields));
            return;
        }
        properties.put(ExternalDataConstants.KEY_REQUESTED_FIELDS,
                serializeExpectedTypeToString(projectionInfo.getProjectionInfo()));
        properties.put(ExternalDataConstants.KEY_HADOOP_ASTERIX_FUNCTION_CALL_INFORMATION,
                serializeFunctionCallInfoToString(projectionInfo.getFunctionCallInfoMap()));
    }

    /**
     * Serialize {@link ARecordType} as Base64 string to pass it to {@link org.apache.hadoop.conf.Configuration}
     *
     * @param expectedType
     *            expected type
     * @return the expected type as Base64 string
     */
    private static String serializeExpectedTypeToString(ARecordType expectedType) throws IOException {
        if (expectedType == DataProjectionInfo.EMPTY_TYPE || expectedType == DataProjectionInfo.ALL_FIELDS_TYPE) {
            //Return the type name of EMPTY_TYPE and ALL_FIELDS_TYPE
            return expectedType.getTypeName();
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        Base64.Encoder encoder = Base64.getEncoder();
        DataProjectionInfo.writeTypeField(expectedType, dataOutputStream);
        return encoder.encodeToString(byteArrayOutputStream.toByteArray());
    }

    /**
     * Serialize {@link FunctionCallInformation} map as Base64 string to pass it to
     * {@link org.apache.hadoop.conf.Configuration}
     *
     * @param functionCallInfoMap
     *            function information map
     * @return function information map as Base64 string
     */
    static String serializeFunctionCallInfoToString(Map<String, FunctionCallInformation> functionCallInfoMap)
            throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        Base64.Encoder encoder = Base64.getEncoder();
        DataProjectionInfo.writeFunctionCallInformationMapField(functionCallInfoMap, dataOutputStream);
        return encoder.encodeToString(byteArrayOutputStream.toByteArray());
    }

    public static int roundUpToNearestFrameSize(int size, int framesize) {
        return ((size / framesize) + 1) * framesize;
    }

    public static int getArgBufferSize() {
        int maxArgSz = DEFAULT_MAX_ARGUMENT_SZ + HEADER_FUDGE;
        String userArgSz = System.getProperty("udf.buf.size");
        if (userArgSz != null) {
            long parsedSize = StorageUtil.getByteValue(userArgSz) + HEADER_FUDGE;
            if (parsedSize < Integer.MAX_VALUE && parsedSize > 0) {
                maxArgSz = (int) parsedSize;
            }
        }
        return maxArgSz;
    }

    public static Optional<String> getFirstNotNull(Map<String, String> configuration, String... parameters) {
        return Arrays.stream(parameters).filter(field -> configuration.get(field) != null).findFirst();
    }

    public static ATypeTag peekArgument(IAType type, IValueReference valueReference, TaggedValuePointable pointy)
            throws HyracksDataException {
        ATypeTag tag = type.getTypeTag();
        if (tag == ATypeTag.ANY) {
            pointy.set(valueReference);
            ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
            IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
            return MessagePackUtils.peekUnknown(rtType);
        } else {
            return MessagePackUtils.peekUnknown(type);
        }
    }

    public static void setVoidArgument(ArrayBackedValueStorage argHolder) throws IOException {
        argHolder.getDataOutput().writeByte(ARRAY16);
        argHolder.getDataOutput().writeShort((short) 0);
    }
}
