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

package org.carbondata.query.executer.pagination.impl;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.carbon.SqlStatement;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.pagination.exception.CarbonPaginationException;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class DataFileChunkHolder {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(DataFileChunkHolder.class.getName());
    /**
     * measureType
     */
    public SqlStatement.Type[] measureType;
    /**
     * temp file
     */
    private File inFile;
    /**
     * read stream
     */
    private DataInputStream stream;
    /**
     * entry count
     */
    private int entryCount;
    /**
     * number record read
     */
    private int numberOfRecordRead;
    /**
     * fileBufferSize
     */
    private int fileBufferSize;
    /**
     * keySize
     */
    private int keySize;
    /**
     * MeasureAggregator
     */
    private MeasureAggregator[] measureAggregator;
    /**
     * byteArrayWrapper
     */
    private ByteArrayWrapper byteArrayWrapper;

    /**
     * CarbonSortTempFileChunkHolder Constructor
     *
     * @param inFile     temp file
     * @param recordSize measure count
     * @param keySize    mdkey length
     */
    public DataFileChunkHolder(File inFile, int keySize, CarbonMetadata.Measure[] measures,
            MeasureAggregator[] measureAggregator, int fileBufferSize) {
        // set temp file 
        this.inFile = inFile;
        // key size
        this.keySize = keySize;
        // measure aggregator
        this.measureAggregator = measureAggregator;
        //fileBufferSize
        this.fileBufferSize = fileBufferSize;
        // byte array wrapper
        this.byteArrayWrapper = new ByteArrayWrapper();
        // out stream
        for (int i = 0; i < measures.length; i++) {
            this.measureType[i] = measures[i].getDataType();
        }
    }

    /**
     * This method will be used to initialize
     *
     * @throws CarbonSortKeyAndGroupByException problem while initializing
     */
    public void initialize() throws CarbonPaginationException {
        // file holder 
        long length = 0;
        FileInputStream in = null;
        try {
            // create reader stream
            in = new FileInputStream(this.inFile);
            FileChannel channel = in.getChannel();
            length = channel.size();
            long position = channel.position();
            channel.position(length - 4);
            ByteBuffer buffer = ByteBuffer.allocate(4);
            channel.read(buffer);
            buffer.rewind();
            //            this.stream = new DataInputStream( new BufferedInputStream(
            //                    new FileInputStream( this.inFile),this.fileBufferSize));
            // read enrty count;
            this.entryCount = buffer.getInt();
            channel.position(position);
            //            channel.close();
            this.stream = new DataInputStream(new BufferedInputStream(in, this.fileBufferSize));
            readRow();
        } catch (Exception e) {
            CarbonUtil.closeStreams(in);
            CarbonUtil.closeStreams(stream);
            throw new CarbonPaginationException(" Problem while reading" + this.inFile + length, e);
        }
    }

    /**
     * This method will be used to read new row from file
     *
     * @throws CarbonSortKeyAndGroupByException problem while reading
     */
    public void readRow() throws CarbonPaginationException {
        try {
            byte[] mdKey = new byte[this.keySize];
            if (this.stream.read(mdKey) < 0) {
                LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                        "Problem while reading mdkey from pagination temp file");
            }
            this.byteArrayWrapper.setMaskedKey(mdKey);
            for (int i = 0; i < this.measureAggregator.length; i++) {
                this.measureAggregator[i].readData(this.stream);
            }
            numberOfRecordRead++;
        } catch (FileNotFoundException e) {
            throw new CarbonPaginationException(this.inFile + " No Found ", e);
        } catch (IOException e) {
            throw new CarbonPaginationException(" Problem while reading" + this.inFile, e);
        }
    }

    /**
     * below method will be used to check whether any more records are present in file or not
     *
     * @return more row present in file
     */
    public boolean hasNext() {
        return this.numberOfRecordRead < this.entryCount;
    }

    /**
     * below method will be used to get the row
     *
     * @return row
     */
    public byte[] getRow() {
        return this.byteArrayWrapper.getMaskedKey();
    }

    /**
     * This method will return the key array
     *
     * @return
     */
    public byte[] getKey() {
        return this.byteArrayWrapper.getMaskedKey();
    }

    /**
     * Below method will be used to close stream
     */
    public void closeStream() {
        CarbonUtil.closeStreams(this.stream);
    }

    public void setMeasureAggs(MeasureAggregator[] aggs) {
        this.measureAggregator = aggs;
    }

    /**
     * This method will return the data array
     *
     * @param index
     * @return
     */
    public MeasureAggregator[] getMeasures() {
        return this.measureAggregator;
    }

}
