/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 */
package org.carbondata.processing.store;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.carbondata.core.datastorage.store.columnar.BlockIndexerStorageForInt;
import org.carbondata.core.datastorage.store.columnar.IndexStorage;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.core.file.manager.composite.FileData;
import org.carbondata.core.file.manager.composite.IFileManagerComposite;
import org.carbondata.core.file.manager.composite.LoadFolderData;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.columnar.ColumnarSplitter;
import org.carbondata.core.keygenerator.columnar.impl.MultiDimKeyVarLengthVariableSplitGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.vo.HybridStoreModel;
import org.carbondata.processing.datatypes.GenericDataType;
import org.carbondata.processing.groupby.CarbonAutoAggGroupBy;
import org.carbondata.processing.groupby.CarbonAutoAggGroupByExtended;
import org.carbondata.processing.groupby.exception.CarbonGroupByException;
import org.carbondata.processing.store.writer.*;
import org.carbondata.processing.store.writer.exception.CarbonDataWriterException;
import org.carbondata.processing.util.CarbonDataProcessorLogEvent;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.processing.util.RemoveDictionaryUtil;
import org.carbondata.query.cache.QueryExecutorUtil;

/**
 * Project Name NSE V3R8C10 
 * Module Name : CARBON Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : CarbonFactDataHandler.java
 * Class Description : Fact data handler class to handle the fact data .  
 * Class Version 1.0
 */
public class CarbonFactDataHandlerColumnar implements CarbonFactHandler {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonFactDataHandlerColumnar.class.getName());

    /**
     * data writer
     */
    private CarbonFactDataWriter dataWriter;

    /**
     * File manager
     */
    private IFileManagerComposite fileManager;

    /**
     * total number of entries in leaf node
     */
    private int entryCount;

    /**
     * startkey of each node
     */
    private byte[] startKey;

    /**
     * end key of each node
     */
    private byte[] endKey;

    /**
     * ValueCompressionModel
     */
    private ValueCompressionModel compressionModel;

    /**
     * data store which will hold the measure data
     */
    private NodeMeasureDataStore dataStore;

    private Map<Integer, GenericDataType> complexIndexMap;

    /**
     * measure count
     */
    private int measureCount;

    /**
     * measure count
     */
    private int dimensionCount;

    /**
     * uniqueValue
     */
    private Object[] uniqueValue;

    /**
     * index of mdkey in incoming rows
     */
    private int mdKeyIndex;

    /**
     * leaf node size
     */
    private int leafNodeSize;

    /**
     * isGroupByEnabled
     */
    private boolean isGroupByEnabled;

    /**
     * groupBy
     */
    private CarbonAutoAggGroupBy groupBy;

    /**
     * mdkeyLength
     */
    private int mdkeyLength;

    /**
     * storeLocation
     */
    private String storeLocation;

    /**
     * schemaName
     */
    private String schemaName;

    /**
     * tableName
     */
    private String tableName;

    /**
     * cubeName
     */
    private String cubeName;

    /**
     * aggregators
     */
    private String[] aggregators;

    /**
     * aggregatorClass
     */
    private String[] aggregatorClass;

    /**
     * CarbonWriteDataHolder
     */
    private CarbonWriteDataHolder[] dataHolder;

    /**
     * factDimLens
     */
    private int[] factDimLens;

    /**
     * isMergingRequest
     */
    private boolean isMergingRequestForCustomAgg;

    /**
     * otherMeasureIndex
     */
    private int[] otherMeasureIndex;

    //    private boolean isUpdateMemberRequest;

    /**
     * customMeasureIndex
     */
    private int[] customMeasureIndex;

    /**
     * dimLens
     */
    private int[] dimLens;

    /**
     * keyGenerator
     */
    private ColumnarSplitter columnarSplitter;

    /**
     * keyBlockHolder
     */
    private CarbonKeyBlockHolder[] keyBlockHolder;

    private boolean isIntBasedIndexer;

    private boolean[] aggKeyBlock;

    private boolean[] isNoDictionary;

    private boolean isAggKeyBlock;

    private long processedDataCount;

    private boolean isCompressedKeyBlock;

    /**
     * factLevels
     */
    private int[] surrogateIndex;

    /**
     * factKeyGenerator
     */
    private KeyGenerator factKeyGenerator;

    /**
     * aggKeyGenerator
     */
    private KeyGenerator keyGenerator;

    private KeyGenerator[] complexKeyGenerator;

    /**
     * maskedByteRanges
     */
    private int[] maskedByte;

    /**
     * isDataWritingRequest
     */
    //    private boolean isDataWritingRequest;

    private ExecutorService writerExecutorService;

    private int numberOfColumns;

    private Object lock = new Object();

    private CarbonWriteDataHolder keyDataHolder;

    private CarbonWriteDataHolder highCardkeyDataHolder;

    private int currentRestructNumber;

    private int highCardCount;

    private HybridStoreModel hybridStoreModel;

    private int[] primitiveDimLens;

    private char[] type;

    private int[] completeDimLens;

    //    private String[] aggregator;

    //TODO SIMIAN

    /**
     * CarbonFactDataHandler cosntructor
     * @param schemaName
     * @param cubeName
     * @param tableName
     * @param isGroupByEnabled
     * @param measureCount
     * @param mdkeyLength
     * @param mdKeyIndex
     * @param aggregators
     * @param aggregatorClass
     * @param highCardCount
     */
    public CarbonFactDataHandlerColumnar(String schemaName, String cubeName, String tableName,
            boolean isGroupByEnabled, int measureCount, int mdkeyLength, int mdKeyIndex,
            String[] aggregators, String[] aggregatorClass, String storeLocation, int[] factDimLens,
            boolean isMergingRequestForCustomAgg, boolean isUpdateMemberRequest, int[] dimLens,
            String[] factLevels, String[] aggLevels, boolean isDataWritingRequest,
            int currentRestructNum, int highCardCount, int dimensionCount,
            Map<Integer, GenericDataType> complexIndexMap, int[] primitiveDimLens,
            HybridStoreModel hybridStoreModel, ValueCompressionModel compressionModel) {
        this(schemaName, cubeName, tableName, isGroupByEnabled, measureCount, mdkeyLength,
                mdKeyIndex, aggregators, aggregatorClass, storeLocation, factDimLens,
                isMergingRequestForCustomAgg, isUpdateMemberRequest, dimLens, factLevels, aggLevels,
                isDataWritingRequest, currentRestructNum, highCardCount, hybridStoreModel,
                compressionModel);
        this.dimensionCount = dimensionCount;
        this.complexIndexMap = complexIndexMap;
        this.primitiveDimLens = primitiveDimLens;
        this.isAggKeyBlock = Boolean.parseBoolean(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK,
                        CarbonCommonConstants.AGGREAGATE_COLUMNAR_KEY_BLOCK_DEFAULTVALUE));

        //row store dimensions will be stored first and than other columnar store dimension will be stored in fact file
        int aggIndex = 0;
        int noDictStartIndex = 0;
        if (this.hybridStoreModel.isHybridStore()) {
            this.aggKeyBlock = new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length
                    + highCardCount + 1 + getComplexColsCount()];
            //hybrid store related changes, row store dimension will not get sorted and hence run length encoding alls will not be applied
            //thus setting aggKeyBlock for row store index as false
            this.aggKeyBlock[aggIndex++] = false;
            this.isNoDictionary = new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length
                    + highCardCount + 1 + getComplexColsCount()];
            noDictStartIndex = this.hybridStoreModel.getColumnStoreOrdinals().length + 1;
        } else {
            //if not hybrid store than as usual
            this.aggKeyBlock = new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length
                    + highCardCount + getComplexColsCount()];
            this.isNoDictionary = new boolean[this.hybridStoreModel.getColumnStoreOrdinals().length
                    + highCardCount + getComplexColsCount()];
            noDictStartIndex = this.hybridStoreModel.getColumnStoreOrdinals().length;
        }
        // setting true value for dims of high card
        for (int i = noDictStartIndex; i < isNoDictionary.length; i++) {
            this.isNoDictionary[i] = true;
        }

        if (isAggKeyBlock) {
            int highCardinalityValue = Integer.parseInt(CarbonProperties.getInstance()
                    .getProperty(CarbonCommonConstants.HIGH_CARDINALITY_VALUE,
                            CarbonCommonConstants.HIGH_CARDINALITY_VALUE_DEFAULTVALUE));
            //since row store index is already set to false, below aggKeyBlock is initialised for remanining dimension
            for (int i = this.hybridStoreModel.getRowStoreOrdinals().length;
                 i < dimLens.length; i++) {
                if (dimLens[i] < highCardinalityValue) {
                    this.aggKeyBlock[aggIndex++] = true;
                    continue;
                }
                aggIndex++;
            }

            if (dimensionCount < dimLens.length) {
                int allColsCount = getColsCount(dimensionCount);
                List<Boolean> aggKeyBlockWithComplex = new ArrayList<Boolean>(allColsCount);
                for (int i = 0; i < dimensionCount; i++) {
                    GenericDataType complexDataType = complexIndexMap.get(i);
                    if (complexDataType != null) {
                        complexDataType.fillAggKeyBlock(aggKeyBlockWithComplex, this.aggKeyBlock);
                    } else {
                        aggKeyBlockWithComplex.add(this.aggKeyBlock[i]);
                    }
                }
                this.aggKeyBlock = new boolean[allColsCount];
                for (int i = 0; i < allColsCount; i++) {
                    this.aggKeyBlock[i] = aggKeyBlockWithComplex.get(i);
                }
            }

        }
    }

    public CarbonFactDataHandlerColumnar(String schemaName, String cubeName, String tableName,
            boolean isGroupByEnabled, int measureCount, int mdkeyLength, int mdKeyIndex,
            String[] aggregators, String[] aggregatorClass, String storeLocation, int[] factDimLens,
            boolean isMergingRequestForCustomAgg, boolean isUpdateMemberRequest, int[] dimLens,
            String[] factLevels, String[] aggLevels, boolean isDataWritingRequest,
            int currentRestructNum, int highCardCount, HybridStoreModel hybridStoreModel,
            ValueCompressionModel compressionModel) {
        this.schemaName = schemaName;
        this.cubeName = cubeName;
        this.tableName = tableName;
        this.storeLocation = storeLocation;
        this.isGroupByEnabled = isGroupByEnabled;
        this.measureCount = measureCount;
        this.mdkeyLength = mdkeyLength;
        this.mdKeyIndex = mdKeyIndex;
        this.aggregators = aggregators;
        this.aggregatorClass = aggregatorClass;
        this.factDimLens = factDimLens;
        this.highCardCount = highCardCount;
        this.isMergingRequestForCustomAgg = isMergingRequestForCustomAgg;
        this.hybridStoreModel = hybridStoreModel;
        //        this.isUpdateMemberRequest=isUpdateMemberRequest;
        this.completeDimLens = dimLens;
        this.dimLens = hybridStoreModel.getHybridCardinality();

        this.currentRestructNumber = currentRestructNum;
        isIntBasedIndexer =
                Boolean.parseBoolean(CarbonCommonConstants.IS_INT_BASED_INDEXER_DEFAULTVALUE);
        this.compressionModel = compressionModel;
        isCompressedKeyBlock =
                Boolean.parseBoolean(CarbonCommonConstants.IS_COMPRESSED_KEYBLOCK_DEFAULTVALUE);

        //        this.isDataWritingRequest=isDataWritingRequest;

        if (this.isGroupByEnabled && isDataWritingRequest && !isUpdateMemberRequest) {
            surrogateIndex = new int[aggLevels.length - highCardCount];
            Arrays.fill(surrogateIndex, -1);
            for (int k = 0; k < aggLevels.length; k++) {
                for (int j = 0; j < factLevels.length; j++) {
                    if (aggLevels[k].equals(factLevels[j])) {
                        surrogateIndex[k] = j;
                        break;
                    }
                }
            }
            this.factKeyGenerator = KeyGeneratorFactory.getKeyGenerator(factDimLens);
            this.keyGenerator = KeyGeneratorFactory.getKeyGenerator(dimLens);
            int[] maskedByteRanges =
                    CarbonDataProcessorUtil.getMaskedByte(surrogateIndex, factKeyGenerator);
            this.maskedByte = new int[factKeyGenerator.getKeySizeInBytes()];
            QueryExecutorUtil.updateMaskedKeyRanges(maskedByte, maskedByteRanges);
        }
        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "Initializing writer executers");
        writerExecutorService = Executors.newFixedThreadPool(3);
    }

    private void setComplexMapSurrogateIndex(int dimensionCount) {
        int surrIndex = 0;
        for (int i = 0; i < complexIndexMap.size(); i++) {
            GenericDataType complexDataType = complexIndexMap.get(i);
            if (complexDataType != null) {
                List<GenericDataType> primitiveTypes = new ArrayList<GenericDataType>();
                complexDataType.getAllPrimitiveChildren(primitiveTypes);
                for (GenericDataType eachPrimitive : primitiveTypes) {
                    eachPrimitive.setSurrogateIndex(surrIndex++);
                }
            } else {
                surrIndex++;
            }
        }
    }

    /**
     * This method will be used to get and update the step properties which will
     * required to run this step
     *
     * @throws CarbonDataWriterException
     *
     */
    public void initialise() throws CarbonDataWriterException {
        fileManager = new LoadFolderData();
        fileManager.setName(new File(this.storeLocation).getName());
        if (!isGroupByEnabled) {
            try {
                setWritingConfiguration(this.mdkeyLength);
            } catch (CarbonDataWriterException ex) {
                throw ex;
            }
        } else if (isGroupByEnabled) {
            setWritingConfiguration(this.keyGenerator.getKeySizeInBytes());
        } else {
            if (!isMergingRequestForCustomAgg) {
                this.groupBy =
                        new CarbonAutoAggGroupBy(aggregators, aggregatorClass, this.schemaName,
                                this.cubeName, this.tableName, this.factDimLens,
                                CarbonCommonConstants.FILE_INPROGRESS_STATUS, currentRestructNumber);
            } else {
                this.groupBy = new CarbonAutoAggGroupByExtended(aggregators, aggregatorClass,
                        this.schemaName, this.cubeName, this.tableName, this.factDimLens,
                        CarbonCommonConstants.FILE_INPROGRESS_STATUS, currentRestructNumber);
            }
        }

    }

    /**
     * This method will add mdkey and measure values to store
     *
     * @param rowData
     * @throws CarbonDataWriterException
     *
     */
    public void addDataToStore(Object[] rowData) throws CarbonDataWriterException {
        if (isGroupByEnabled) {
            rowData[mdKeyIndex] = getAggregateTableMdkey((byte[]) rowData[mdKeyIndex]);
            addToStore(rowData);
        } else {
            addToStore(rowData);
        }
    }

    /**
     * below method will be used to add row to store
     * @param row
     * @throws CarbonDataWriterException
     */
    private void addToStore(Object[] row) throws CarbonDataWriterException {
        byte[] mdkey = (byte[]) row[this.mdKeyIndex];
        byte[] highCardKey = null;
        if (highCardCount > 0 || complexIndexMap.size() > 0) {
            highCardKey = (byte[]) row[this.mdKeyIndex - 1];
        }
        ByteBuffer byteBuffer = null;
        byte[] b = null;
        if (this.entryCount == 0) {
            this.startKey = mdkey;
        }
        this.endKey = mdkey;
        // add to key store
        if (mdkey.length > 0) {
            keyDataHolder.setWritableByteArrayValueByIndex(entryCount, mdkey);
        }

        // for storing the byte [] for high card.
        if (highCardCount > 0 || complexIndexMap.size() > 0) {
            highCardkeyDataHolder.setWritableByteArrayValueByIndex(entryCount, highCardKey);
        }
        //Add all columns to keyDataHolder
        keyDataHolder.setWritableByteArrayValueByIndex(entryCount, this.mdKeyIndex, row);

        // CHECKSTYLE:OFF Approval No:Approval-351
        for (int k = 0; k < otherMeasureIndex.length; k++) {
            if (type[otherMeasureIndex[k]] == CarbonCommonConstants.BIG_INT_MEASURE) {
                if (null == row[otherMeasureIndex[k]]) {
                    dataHolder[otherMeasureIndex[k]].setWritableLongValueByIndex(entryCount,
                            uniqueValue[otherMeasureIndex[k]]);
                } else {
                    dataHolder[otherMeasureIndex[k]]
                            .setWritableLongValueByIndex(entryCount, row[otherMeasureIndex[k]]);
                }
            } else {
                if (null == row[otherMeasureIndex[k]]) {
                    dataHolder[otherMeasureIndex[k]].setWritableDoubleValueByIndex(entryCount,
                            uniqueValue[otherMeasureIndex[k]]);
                } else {
                    dataHolder[otherMeasureIndex[k]]
                            .setWritableDoubleValueByIndex(entryCount, row[otherMeasureIndex[k]]);
                }
            }
        }
        for (int i = 0; i < customMeasureIndex.length; i++) {
            if (null == row[customMeasureIndex[i]]
                    && type[customMeasureIndex[i]] == CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                BigDecimal val = (BigDecimal) uniqueValue[customMeasureIndex[i]];
                b = DataTypeUtil.bigDecimalToByte(val);
            } else {
                b = (byte[]) row[customMeasureIndex[i]];
            }
            byteBuffer = ByteBuffer.allocate(b.length + CarbonCommonConstants.INT_SIZE_IN_BYTE);
            byteBuffer.putInt(b.length);
            byteBuffer.put(b);
            byteBuffer.flip();
            b = byteBuffer.array();
            dataHolder[customMeasureIndex[i]].setWritableByteArrayValueByIndex(entryCount, b);
        }
        this.entryCount++;
        // if entry count reaches to leaf node size then we are ready to
        // write
        // this to leaf node file and update the intermediate files
        if (this.entryCount == this.leafNodeSize) {
            //            byte[][][] data = new byte[numberOfColumns][][];
            //            for(int i = 0;i < keyBlockHolder.length;i++)
            //            {
            //                data[i]=keyBlockHolder[i].getKeyBlock().clone();
            //            }
            byte[][] byteArrayValues = keyDataHolder.getByteArrayValues().clone();
            byte[][][] columnByteArrayValues = keyDataHolder.getColumnByteArrayValues().clone();
            //TODO need to handle high card also here

            byte[][] writableMeasureDataArray =
                    this.dataStore.getWritableMeasureDataArray(dataHolder).clone();
            int entryCountLocal = entryCount;
            byte[] startKeyLocal = startKey;
            byte[] endKeyLocal = endKey;
            startKey = new byte[mdkeyLength];
            endKey = new byte[mdkeyLength];
            //            writerExecutorService.submit(new DataWriterThread(byteArrayValues,writableMeasureDataArray,entryCountLocal,startKeyLocal,endKeyLocal));
            writerExecutorService
                    .submit(new DataWriterThread(byteArrayValues, writableMeasureDataArray,
                            columnByteArrayValues, entryCountLocal, startKeyLocal, endKeyLocal));
            //            writeDataToFile(data,writableMeasureDataArray,entryCount,startKey,endKey);
            // set the entry count to zero
            processedDataCount += entryCount;
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "*******************************************Number Of records processed: "
                            + processedDataCount);
            this.entryCount = 0;
            resetKeyBlockHolder();
            initialisedataHolder();
            keyDataHolder.reset();
        }
    }

    //    private void writeDataToFile(byte[][] data,
    //            byte[][] dataHolderLocal, int entryCountLocal,
    //            byte[] startkeyLocal, byte[] endKeyLocal)
    //            throws CarbonDataWriterException
    //    {
    //        ExecutorService executorService= Executors.newFixedThreadPool(5);
    //        List<Future<IndexStorage>> submit = new ArrayList<Future<IndexStorage>>(numberOfColumns);
    //        byte[][][] columnsData = new byte[numberOfColumns][data.length][];
    //        for(int i = 0;i < data.length;i++)
    //        {
    //            byte[][] splitKey = columnarSplitter.splitKey(data[i]);
    //            for(int j = 0;j < splitKey.length;j++)
    //            {
    //                columnsData[j][i]=splitKey[j];
    //            }
    //        }
    //        for(int i = 0;i < numberOfColumns;i++)
    //        {
    //            submit.add(executorService.submit(new BlockSortThread(i,columnsData[i])));
    //        }
    //        executorService.shutdown();
    //        try
    //        {
    //            executorService.awaitTermination(1, TimeUnit.DAYS);
    //        }
    //        catch(InterruptedException ex)
    //        {
    //            // TODO Auto-generated catch block
    //         //   e.printStackTrace();
    //            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, ex, ex.getMessage());
    //        }
    //        IndexStorage[] blockStorage = new IndexStorage[numberOfColumns];
    //        try
    //        {
    //            for(int i = 0;i < blockStorage.length;i++)
    //            {
    //                blockStorage[i]=submit.get(i).get();
    //            }
    //        }
    //        catch(Exception exception)
    //        {
    //            // TODO Auto-generated catch block
    ////            e.printStackTrace();
    //        	 LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, exception, exception.getMessage());
    //        }
    //        synchronized(lock)
    //        {
    //            this.dataWriter.writeDataToFile(
    //                    blockStorage,
    //                    dataHolderLocal,
    //                    entryCountLocal, startkeyLocal, endKeyLocal);
    //        }
    //    }

    private void writeDataToFile(byte[][] data, byte[][] dataHolderLocal, byte[][][] columnData,
            int entryCountLocal, byte[] startkeyLocal, byte[] endKeyLocal)
            throws CarbonDataWriterException {
        //    	for(int i = 0;i < data.length;i++)
        //    	{
        //    		byte[][] splitKey = columnarSplitter.splitKey(data[i]);
        //    		for(int j = 0;j < splitKey.length;j++)
        //    		{
        //    			columnsData[j][i]=splitKey[j];
        //    		}
        //    	}
        int allColsCount = getColsCount(hybridStoreModel.getColumnSplit().length);
        List<ArrayList<byte[]>> colsAndValues = new ArrayList<ArrayList<byte[]>>();
        for (int i = 0; i < allColsCount; i++) {
            colsAndValues.add(new ArrayList<byte[]>());
        }

        for (int i = 0; i < columnData.length; i++) {
            int l = 0;
            for (int j = 0; j < dimensionCount; j++) {
                GenericDataType complexDataType = complexIndexMap.get(j);
                if (complexDataType != null) {
                    List<ArrayList<byte[]>> columnsArray = new ArrayList<ArrayList<byte[]>>();
                    for (int k = 0; k < complexDataType.getColsCount(); k++) {
                        columnsArray.add(new ArrayList<byte[]>());
                    }
                    complexDataType.getColumnarDataForComplexType(columnsArray,
                            ByteBuffer.wrap(columnData[i][j]));
                    for (ArrayList<byte[]> eachColumn : columnsArray) {
                        colsAndValues.get(l++).addAll(eachColumn);
                    }
                } else {
                    colsAndValues.get(l++).add(columnData[i][j]);
                }
            }
        }

        //    	for(int i = 0;i < numberOfColumns;i++)
        //    	{
        //    		submit.add(executorService.submit(new BlockSortThread(i,columnsData[i])));
        //    	}

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        List<Future<IndexStorage>> submit = new ArrayList<Future<IndexStorage>>(allColsCount);
        int l = 0;
        for (int j = 0; j < dimensionCount; j++) {
            GenericDataType complexDataType = complexIndexMap.get(j);
            if (complexDataType != null) {
                for (int k = 0; k < complexDataType.getColsCount(); k++) {
                    submit.add(executorService.submit(new BlockSortThread(l,
                            colsAndValues.get(l).toArray(new byte[colsAndValues.get(l++).size()][]),
                            false)));
                }
            } else {
                submit.add(executorService.submit(new BlockSortThread(l,
                        colsAndValues.get(l).toArray(new byte[colsAndValues.get(l++).size()][]),
                        true)));
            }
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException ex) {
            // TODO Auto-generated catch block
            //   e.printStackTrace();
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, ex,
                    ex.getMessage());
        }
        IndexStorage[] blockStorage = new IndexStorage[numberOfColumns];
        try {
            for (int i = 0; i < blockStorage.length; i++) {
                blockStorage[i] = submit.get(i).get();
            }
        } catch (Exception exception) {
            // TODO Auto-generated catch block
            //            e.printStackTrace();
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, exception,
                    exception.getMessage());
        }
        synchronized (lock) {
            this.dataWriter
                    .writeDataToFile(blockStorage, dataHolderLocal, entryCountLocal, startkeyLocal,
                            endKeyLocal);
        }
    }

    /**
     * below method will be used to finish the data handler
     * @throws CarbonDataWriterException
     */
    public void finish() throws CarbonDataWriterException {
        //        if(isGroupByEnabled && !this.isUpdateMemberRequest)
        //        {
        //            try
        //            {
        //                this.groupBy
        //                        .initiateReading(this.storeLocation, this.tableName);
        //                setWritingConfiguration(this.keyGenerator.getKeySizeInBytes());
        //                //CHECKSTYLE:OFF    Approval No:Approval-V3R8C00_018
        //                Object[] rowObj= null;
        //                while(this.groupBy.hasNext())
        //                { //CHECKSTYLE:ON
        //                    rowObj = this.groupBy.next();
        //                    if(isDataWritingRequest)
        //                    {
        //                        rowObj[mdKeyIndex]=getAggregateTableMdkey((byte[])rowObj[mdKeyIndex]);
        //                    }
        //                    addToStore(rowObj);
        //                }
        //            }
        //            catch(CarbonGroupByException e)
        //            {
        //                throw new CarbonDataWriterException(
        //                        "Problem while doing the groupby", e);
        //            }
        //            finally
        //            {
        //                try
        //                {
        //                    this.groupBy.finish();
        //                }
        //                catch(CarbonGroupByException e)
        //                {
        //                    LOGGER.error(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, "Problem in group by finish");
        //                }
        //            }
        //        }
        // / still some data is present in stores if entryCount is more
        // than 0
        if (this.entryCount > 0) {
            // write data to file
            //            for(int i = 0;i < columnarCompressedData.length;i++)
            //            {
            //                columnarCompressedData[i].compress(keyBlockHolder[i].getKeyBlock());
            //            }
            int noOfColumn = hybridStoreModel.getNoOfColumnStore();
            byte[][] data = keyDataHolder.getByteArrayValues();

            byte[][][] columnsData = new byte[noOfColumn][data.length][];
            for (int i = 0; i < data.length; i++) {
                byte[][] splitKey = columnarSplitter.splitKey(data[i]);
                for (int j = 0; j < splitKey.length; j++) {
                    columnsData[j][i] = splitKey[j];
                }
            }

            byte[][][] highCardColumnsData = null;
            List<ArrayList<byte[]>> colsAndValues = new ArrayList<ArrayList<byte[]>>();
            int complexColCount = getComplexColsCount();

            for (int i = 0; i < complexColCount; i++) {
                colsAndValues.add(new ArrayList<byte[]>());
            }

            if (highCardCount > 0 || complexIndexMap.size() > 0) {
                byte[][] highCardData = highCardkeyDataHolder.getByteArrayValues();

                highCardColumnsData = new byte[highCardCount][highCardData.length][];

                for (int i = 0; i < highCardData.length; i++) {
                    int complexColumnIndex = primitiveDimLens.length + highCardCount;
                    byte[][] splitKey = RemoveDictionaryUtil.splitHighCardKey(highCardData[i],
                            highCardCount + complexIndexMap.size());

                    int complexTypeIndex = 0;
                    for (int j = 0; j < splitKey.length; j++) {
                        //nodictionary Columns
                        if (j < highCardCount) {
                            highCardColumnsData[j][i] = splitKey[j];
                        }
                        //complex types
                        else {
                            //Need to write columnar block from complex byte array
                            GenericDataType complexDataType =
                                    complexIndexMap.get(complexColumnIndex++);
                            if (complexDataType != null) {
                                List<ArrayList<byte[]>> columnsArray =
                                        new ArrayList<ArrayList<byte[]>>();
                                for (int k = 0; k < complexDataType.getColsCount(); k++) {
                                    columnsArray.add(new ArrayList<byte[]>());
                                }

                                try {
                                    ByteBuffer complexDataWithoutBitPacking =
                                            ByteBuffer.wrap(splitKey[j]);
                                    byte[] complexTypeData =
                                            new byte[complexDataWithoutBitPacking.getShort()];
                                    complexDataWithoutBitPacking.get(complexTypeData);

                                    ByteBuffer byteArrayInput = ByteBuffer.wrap(complexTypeData);
                                    ByteArrayOutputStream byteArrayOutput =
                                            new ByteArrayOutputStream();
                                    DataOutputStream dataOutputStream =
                                            new DataOutputStream(byteArrayOutput);
                                    complexDataType
                                            .parseAndBitPack(byteArrayInput, dataOutputStream,
                                                    this.complexKeyGenerator);
                                    complexDataType.getColumnarDataForComplexType(columnsArray,
                                            ByteBuffer.wrap(byteArrayOutput.toByteArray()));
                                    byteArrayOutput.close();
                                } catch (IOException e) {
                                    throw new CarbonDataWriterException(
                                            "Problem while bit packing and writing complex datatype",
                                            e);
                                } catch (KeyGenException e) {
                                    throw new CarbonDataWriterException(
                                            "Problem while bit packing and writing complex datatype",
                                            e);
                                }

                                for (ArrayList<byte[]> eachColumn : columnsArray) {
                                    colsAndValues.get(complexTypeIndex++).addAll(eachColumn);
                                }
                            } else {
                                // This case not possible as ComplexType is the last columns
                            }
                        }
                    }
                }
            }
            ExecutorService executorService = Executors.newFixedThreadPool(7);
            List<Future<IndexStorage>> submit = new ArrayList<Future<IndexStorage>>(
                    primitiveDimLens.length + highCardCount + complexColCount);
            int i = 0;
            for (i = 0; i < noOfColumn; i++) {
                submit.add(executorService.submit(new BlockSortThread(i, columnsData[i], true)));
            }
            for (int j = 0; j < highCardCount; j++) {
                submit.add(executorService
                        .submit(new BlockSortThread(i++, highCardColumnsData[j], false, true,
                                true)));
            }
            for (int k = 0; k < complexColCount; k++) {
                submit.add(executorService.submit(new BlockSortThread(i++,
                        colsAndValues.get(k).toArray(new byte[colsAndValues.get(k).size()][]),
                        false)));
            }

            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                //                e.printStackTrace();
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                        e.getMessage());
            }
            IndexStorage[] blockStorage =
                    new IndexStorage[noOfColumn + highCardCount + complexColCount];
            try {
                for (int k = 0; k < blockStorage.length; k++) {
                    blockStorage[k] = submit.get(k).get();
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                //                e.printStackTrace();
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                        e.getMessage());
            }

            writerExecutorService.shutdown();
            try {
                writerExecutorService.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                //                e.printStackTrace();
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG, e,
                        e.getMessage());
            }
            this.dataWriter.writeDataToFile(blockStorage,
                    this.dataStore.getWritableMeasureDataArray(dataHolder), this.entryCount,
                    this.startKey, this.endKey);

            processedDataCount += entryCount;
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "*******************************************Number Of records processed: "
                            + processedDataCount);
            this.dataWriter.writeleafMetaDataToFile();
        } else if (null != this.dataWriter && this.dataWriter.getLeafMetadataSize() > 0) {
            this.dataWriter.writeleafMetaDataToFile();
        }
    }

    //TODO SIMIAN
    private byte[] getAggregateTableMdkey(byte[] maksedKey) throws CarbonDataWriterException {
        long[] keyArray = this.factKeyGenerator.getKeyArray(maksedKey, maskedByte);

        int[] aggSurrogateKey = new int[surrogateIndex.length];

        for (int j = 0; j < aggSurrogateKey.length; j++) {
            aggSurrogateKey[j] = (int) keyArray[surrogateIndex[j]];
        }

        try {
            return keyGenerator.generateKey(aggSurrogateKey);
        } catch (KeyGenException e) {
            throw new CarbonDataWriterException("Problem while generating the mdkeyfor aggregate ",
                    e);
        }
    }

    private int getColsCount(int columnSplit) {
        int count = 0;
        for (int i = 0; i < columnSplit; i++) {
            GenericDataType complexDataType = complexIndexMap.get(i);
            if (complexDataType != null) {
                count += complexDataType.getColsCount();
            } else count++;
        }
        return count;
    }

    private int getComplexColsCount() {
        int count = 0;
        for (int i = 0; i < dimensionCount; i++) {
            GenericDataType complexDataType = complexIndexMap.get(i);
            if (complexDataType != null) {
                count += complexDataType.getColsCount();
            }
        }
        return count;
    }

    /**
     * below method will be used to close the handler
     */
    public void closeHandler() {
        if (null != this.dataWriter) {
            // close all the open stream for both the files
            this.dataWriter.closeWriter();
            int size = fileManager.size();
            FileData fileData = null;
            String storePath = null;
            String inProgFileName = null;
            String changedFileName = null;
            File currntFile = null;
            File destFile = null;
            for (int i = 0; i < size; i++) {
                fileData = (FileData) fileManager.get(i);

                storePath = fileData.getStorePath();
                inProgFileName = fileData.getFileName();
                changedFileName = inProgFileName.substring(0, inProgFileName.lastIndexOf('.'));
                currntFile = new File(storePath + File.separator + inProgFileName);
                destFile = new File(storePath + File.separator + changedFileName);
                if (!currntFile.renameTo(destFile)) {
                    LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                            "Problem while renaming the file");
                }
                fileData.setName(changedFileName);
            }
        }
        if (null != groupBy) {
            try {
                this.groupBy.finish();
            } catch (CarbonGroupByException ex) {
                LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                        "Problem while closing the groupby file");
            }
        }
        this.dataWriter = null;
        this.groupBy = null;
        this.keyBlockHolder = null;
        this.dataStore = null;

    }

    /**
     * Below method will be to configure fact file writing configuration
     * @throws CarbonDataWriterException
     */
    private void setWritingConfiguration(int mdkeySize) throws CarbonDataWriterException {
        this.uniqueValue = compressionModel.getUniqueValue();
        // get leaf node size
        this.leafNodeSize = Integer.parseInt(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.LEAFNODE_SIZE,
                        CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));

        LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                "************* Leaf Node Size: " + leafNodeSize);

        //        boolean isColumnar=Boolean.parseBoolean(CarbonProperties.getInstance().getProperty(
        //                CarbonCommonConstants.IS_COLUMNAR_STORAGE,
        //                CarbonCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE));
        //
        int dimSet = Integer.parseInt(
                CarbonCommonConstants.DIMENSION_SPLIT_VALUE_IN_COLUMNAR_DEFAULTVALUE);
        //
        //        if(!isColumnar)
        //        {
        //        	dimSet=dimLens.length;
        //        }
        // if atleast one dimension is present then initialize column splitter otherwise null

        int[] keyBlockSize = null;
        if (dimLens.length > 0) {
            //Using Variable length variable split generator
            //This will help in splitting mdkey to columns. variable split is required because all columns which are part of
            //row store will be in single column store
            //e.g if {0,1,2,3,4,5} is dimension and {0,1,2) is row store dimension
            //than below splitter will return column as {0,1,2}{3}{4}{5}
            this.columnarSplitter = new MultiDimKeyVarLengthVariableSplitGenerator(CarbonUtil
                    .getDimensionBitLength(hybridStoreModel.getHybridCardinality(),
                            hybridStoreModel.getDimensionPartitioner()),
                    hybridStoreModel.getColumnSplit());
            this.keyBlockHolder =
                    new CarbonKeyBlockHolder[this.columnarSplitter.getBlockKeySize().length];
            keyBlockSize = columnarSplitter.getBlockKeySize();
            this.complexKeyGenerator = new KeyGenerator[completeDimLens.length];
            for (int i = 0; i < completeDimLens.length; i++) {
                complexKeyGenerator[i] =
                        KeyGeneratorFactory.getKeyGenerator(new int[] { completeDimLens[i] });
            }
        } else {
            keyBlockSize = new int[0];
            this.keyBlockHolder = new CarbonKeyBlockHolder[0];
        }

        for (int i = 0; i < keyBlockHolder.length; i++) {
            this.keyBlockHolder[i] = new CarbonKeyBlockHolder(leafNodeSize);
            this.keyBlockHolder[i].resetCounter();
        }

        numberOfColumns = keyBlockHolder.length;

        // create data store
        this.dataStore = StoreFactory.createDataStore(compressionModel);
        // agg type
        type = compressionModel.getType();
        List<Integer> otherMeasureIndexList =
                new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<Integer> customMeasureIndexList =
                new ArrayList<Integer>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        for (int j = 0; j < type.length; j++) {
            if (type[j] != 'c') {
                otherMeasureIndexList.add(j);
            } else {
                customMeasureIndexList.add(j);
            }
        }
        otherMeasureIndex = new int[otherMeasureIndexList.size()];
        customMeasureIndex = new int[customMeasureIndexList.size()];
        for (int i = 0; i < otherMeasureIndex.length; i++) {
            otherMeasureIndex[i] = otherMeasureIndexList.get(i);
        }
        for (int i = 0; i < customMeasureIndex.length; i++) {
            customMeasureIndex[i] = customMeasureIndexList.get(i);
        }

        this.dataHolder = new CarbonWriteDataHolder[this.measureCount];
        for (int i = 0; i < otherMeasureIndex.length; i++) {
            this.dataHolder[otherMeasureIndex[i]] = new CarbonWriteDataHolder();
            if (type[otherMeasureIndex[i]] == CarbonCommonConstants.BIG_INT_MEASURE) {
                this.dataHolder[otherMeasureIndex[i]].initialiseLongValues(this.leafNodeSize);
            } else {
                this.dataHolder[otherMeasureIndex[i]].initialiseDoubleValues(this.leafNodeSize);
            }
        }
        for (int i = 0; i < customMeasureIndex.length; i++) {
            this.dataHolder[customMeasureIndex[i]] = new CarbonWriteDataHolder();
            this.dataHolder[customMeasureIndex[i]].initialiseByteArrayValues(leafNodeSize);
        }

        keyDataHolder = new CarbonWriteDataHolder();
        keyDataHolder.initialiseByteArrayValues(leafNodeSize);
        highCardkeyDataHolder = new CarbonWriteDataHolder();
        highCardkeyDataHolder.initialiseByteArrayValues(leafNodeSize);

        initialisedataHolder();
        // create data writer instance
        //        this.dataWriter = new CarbonFactDataWriterImpl(this.storeLocation,
        //                this.measureCount, this.mdkeyLength, this.tableName,true,fileManager, this.columnarSplitter.getBlockKeySize());
        //CarbonUtil.getDimensionBitLength(hybridStoreModel.getHybridCardinality(),hybridStoreModel.getDimensionPartitioner()),hybridStoreModel.getColumnSplit()
        //complex data type
        /*int[] blockKeySize = getBlockKeySizeWithComplexTypes(new MultiDimKeyVarLengthEquiSplitGenerator(
                CarbonUtil.getIncrementedCardinalityFullyFilled(dimLens.clone()),(byte)dimSet).getBlockKeySize());*/
        setComplexMapSurrogateIndex(this.dimensionCount);
        this.dataWriter = getFactDataWriter(this.storeLocation, this.measureCount, this.mdkeyLength,
                this.tableName, true, fileManager, keyBlockSize);
        this.dataWriter.setIsNoDictionary(isNoDictionary);
        // initialize the channel;
        this.dataWriter.initializeWriter();

    }

    private void resetKeyBlockHolder() {
        for (int i = 0; i < keyBlockHolder.length; i++) {
            this.keyBlockHolder[i].resetCounter();
        }
    }

    private void initialisedataHolder() {
        //        this.dataHolder= new CarbonWriteDataHolder[this.measureCount];

        for (int i = 0; i < this.dataHolder.length; i++) {
            this.dataHolder[i].reset();
        }

        //        for(int i = 0;i < otherMeasureIndex.length;i++)
        //        {
        //            this.dataHolder[otherMeasureIndex[i]]=new CarbonWriteDataHolder();
        //            this.dataHolder[otherMeasureIndex[i]].initialiseDoubleValues(this.leafNodeSize);
        //        }
        //        for(int i = 0;i < customMeasureIndex.length;i++)
        //        {
        //            this.dataHolder[customMeasureIndex[i]]=new CarbonWriteDataHolder();
        //            this.dataHolder[customMeasureIndex[i]].initialiseByteArrayValues(leafNodeSize);
        //        }
    }

    private CarbonFactDataWriter<?> getFactDataWriter(String storeLocation, int measureCount,
            int mdKeyLength, String tableName, boolean isNodeHolder,
            IFileManagerComposite fileManager, int[] keyBlockSize) {

        if (isCompressedKeyBlock && isIntBasedIndexer && isAggKeyBlock) {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "**************************Compressed key block and aggregated and int");
            return new CarbonFactDataWriterImplForIntIndexAndAggBlockCompressed(storeLocation,
                    measureCount, mdKeyLength, tableName, isNodeHolder, fileManager, keyBlockSize,
                    aggKeyBlock, dimLens, false);
        } else if (isIntBasedIndexer && isAggKeyBlock) {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "*************************************aggregated and int");
            return new CarbonFactDataWriterImplForIntIndexAndAggBlock(storeLocation, measureCount,
                    mdKeyLength, tableName, isNodeHolder, fileManager, keyBlockSize, aggKeyBlock,
                    false, isComplexTypes(), highCardCount);
        } else if (isIntBasedIndexer) {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "************************************************int");
            return new CarbonFactDataWriterImplForIntIndex(storeLocation, measureCount, mdKeyLength,
                    tableName, isNodeHolder, fileManager, keyBlockSize, false);
        } else {
            LOGGER.info(CarbonDataProcessorLogEvent.UNIBI_CARBONDATAPROCESSOR_MSG,
                    "************************************************short");
            return new CarbonFactDataWriterImpl(storeLocation, measureCount, mdKeyLength, tableName,
                    isNodeHolder, fileManager, keyBlockSize, false);
        }
    }

    private int[] getBlockKeySizeWithComplexTypes(int[] primitiveBlockKeySize) {
        int allColsCount = getColsCount(
                hybridStoreModel.getColumnSplit().length + highCardCount + complexIndexMap.size());
        int[] blockKeySizeWithComplexTypes = new int[allColsCount];

        List<Integer> blockKeySizeWithComplex = new ArrayList<Integer>(allColsCount);
        for (int i = 0; i < primitiveBlockKeySize.length; i++) {
            GenericDataType complexDataType = complexIndexMap.get(i);
            if (complexDataType != null) {
                complexDataType.fillBlockKeySize(blockKeySizeWithComplex, primitiveBlockKeySize);
            } else {
                blockKeySizeWithComplex.add(primitiveBlockKeySize[i]);
            }
        }
        for (int i = 0; i < allColsCount; i++) {
            blockKeySizeWithComplexTypes[i] = blockKeySizeWithComplex.get(i);
        }

        return blockKeySizeWithComplexTypes;
    }

    private boolean[] isComplexTypes() {
        int noOfColumn =
                hybridStoreModel.getNoOfColumnStore() + highCardCount + complexIndexMap.size();
        int allColsCount = getColsCount(noOfColumn);
        boolean[] isComplexType = new boolean[allColsCount];

        List<Boolean> complexTypesList = new ArrayList<Boolean>(allColsCount);
        for (int i = 0; i < noOfColumn; i++) {
            GenericDataType complexDataType = complexIndexMap.get(i);
            if (complexDataType != null) {
                int count = complexDataType.getColsCount();
                for (int j = 0; j < count; j++) {
                    complexTypesList.add(true);
                }
            } else {
                complexTypesList.add(false);
            }
        }
        for (int i = 0; i < allColsCount; i++) {
            isComplexType[i] = complexTypesList.get(i);
        }

        return isComplexType;
    }

    private final class DataWriterThread implements Callable<IndexStorage> {
        private byte[][] data;

        private byte[][][] columnData;

        private byte[][] dataHolderLocal;

        private int entryCountLocal;

        private byte[] startkeyLocal;

        private byte[] endKeyLocal;

        private DataWriterThread(byte[][] data, byte[][] dataHolderLocal, int entryCountLocal,
                byte[] startKey, byte[] endKey) {
            this.data = data;
            this.entryCountLocal = entryCountLocal;
            this.startkeyLocal = startKey;
            this.endKeyLocal = endKey;
            this.dataHolderLocal = dataHolderLocal;
        }

        private DataWriterThread(byte[][] data, byte[][] dataHolderLocal, byte[][][] columnData,
                int entryCountLocal, byte[] startKey, byte[] endKey) {
            this.data = data;
            this.columnData = columnData;
            this.entryCountLocal = entryCountLocal;
            this.startkeyLocal = startKey;
            this.endKeyLocal = endKey;
            this.dataHolderLocal = dataHolderLocal;
        }

        @Override
        public IndexStorage call() throws Exception {
            //            writeDataToFile(this.data,dataHolderLocal, entryCountLocal,startkeyLocal,endKeyLocal);
            writeDataToFile(this.data, dataHolderLocal, columnData, entryCountLocal, startkeyLocal,
                    endKeyLocal);
            return null;
        }

    }

    private final class BlockSortThread implements Callable<IndexStorage> {
        private int index;

        private byte[][] data;
        private boolean isSortRequired;
        private boolean isCompressionReq;

        private boolean isHighCardinality;

        private boolean isRowBlock;

        private BlockSortThread(int index, byte[][] data, boolean isSortRequired) {
            this.index = index;
            this.data = data;
            isCompressionReq = aggKeyBlock[this.index];
            this.isSortRequired = isSortRequired;
            if (hybridStoreModel.isHybridStore() && this.index == 0) {
                isRowBlock = true;
            }

        }

        public BlockSortThread(int index, byte[][] data, boolean b, boolean isHighCardinality,
                boolean isSortRequired) {
            this.index = index;
            this.data = data;
            isCompressionReq = b;
            this.isHighCardinality = isHighCardinality;
            this.isSortRequired = isSortRequired;
            if (hybridStoreModel.isHybridStore() && this.index == 0) {
                isRowBlock = true;
            }
        }

        @Override
        public IndexStorage call() throws Exception {
            return new BlockIndexerStorageForInt(this.data, isCompressionReq, isHighCardinality,
                    isSortRequired, isRowBlock);

        }

    }
}
