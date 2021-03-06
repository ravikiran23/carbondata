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

package org.carbondata.core.datastorage.store.filesystem;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonCoreLogEvent;

public class HDFSCarbonFile implements CarbonFile {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(HDFSCarbonFile.class.getName());
    private FileStatus fileStatus;
    private FileSystem fs;

    public HDFSCarbonFile(String filePath) {
        filePath = filePath.replace("\\", "/");
        Path path = new Path(filePath);
        try {
            fs = path.getFileSystem(FileFactory.getConfiguration());
            fileStatus = fs.getFileStatus(path);
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + e.getMessage());
        }
    }

    public HDFSCarbonFile(Path path) {
        try {
            fs = path.getFileSystem(FileFactory.getConfiguration());
            fileStatus = fs.getFileStatus(path);
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + e.getMessage());
        }
    }

    public HDFSCarbonFile(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    @Override
    public boolean createNewFile() {
        Path path = fileStatus.getPath();
        try {
            return fs.createNewFile(path);
        } catch (IOException e) {
            return false;
        }

    }

    @Override
    public String getAbsolutePath() {
        return fileStatus.getPath().toString();
    }

    @Override
    public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
        FileStatus[] listStatus = null;
        try {
            if (null != fileStatus && fileStatus.isDir()) {
                Path path = fileStatus.getPath();
                listStatus = path.getFileSystem(FileFactory.getConfiguration())
                        .listStatus(path, new PathFilter() {

                            @Override
                            public boolean accept(Path path) {

                                return fileFilter.accept(new HDFSCarbonFile(path));
                            }
                        });
            } else {
                return null;
            }
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + e.getMessage());
            return new CarbonFile[0];
        }

        return getFiles(listStatus);
    }

    @Override
    public String getName() {
        return fileStatus.getPath().getName();
    }

    @Override
    public boolean isDirectory() {
        return fileStatus.isDir();
    }

    @Override
    public boolean exists() {
        try {
            if (null != fileStatus) {
                fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
                return fs.exists(fileStatus.getPath());
            }
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + e.getMessage());
        }
        return false;
    }

    @Override
    public String getCanonicalPath() {
        return getAbsolutePath();
    }

    @Override
    public CarbonFile getParentFile() {
        return new HDFSCarbonFile(fileStatus.getPath().getParent());
    }

    @Override
    public String getPath() {
        return getAbsolutePath();
    }

    @Override
    public long getSize() {
        return fileStatus.getLen();
    }

    public boolean renameTo(String changetoName) {
        FileSystem fs;
        try {
            fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
            return fs.rename(fileStatus.getPath(), new Path(changetoName));
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + e.getMessage());
            return false;
        }
    }

    public boolean delete() {
        FileSystem fs;
        try {
            fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
            return fs.delete(fileStatus.getPath(), true);
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + e.getMessage());
            return false;
        }
    }

    @Override
    public CarbonFile[] listFiles() {

        FileStatus[] listStatus = null;
        try {
            if (null != fileStatus && fileStatus.isDir()) {
                Path path = fileStatus.getPath();
                listStatus = path.getFileSystem(FileFactory.getConfiguration()).listStatus(path);
            } else {
                return null;
            }
        } catch (IOException ex) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + ex.getMessage());
            return new CarbonFile[0];
        }

        return getFiles(listStatus);
    }

    /**
     * @param listStatus
     * @return
     */
    private CarbonFile[] getFiles(FileStatus[] listStatus) {
        if (listStatus == null) {
            return new CarbonFile[0];
        }

        CarbonFile[] files = new CarbonFile[listStatus.length];

        for (int i = 0; i < files.length; i++) {
            files[i] = new HDFSCarbonFile(listStatus[i]);
        }
        return files;
    }

    @Override
    public boolean mkdirs() {
        Path path = fileStatus.getPath();
        try {
            return fs.mkdirs(path);
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public long getLastModifiedTime() {
        return fileStatus.getModificationTime();
    }

    @Override
    public boolean setLastModifiedTime(long timestamp) {
        try {
            fs.setTimes(fileStatus.getPath(), timestamp, timestamp);
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean renameForce(String changetoName) {
        FileSystem fs;
        try {
            fs = fileStatus.getPath().getFileSystem(FileFactory.getConfiguration());
            if (fs instanceof DistributedFileSystem) {
                ((DistributedFileSystem) fs).
                        rename(fileStatus.getPath(), new Path(changetoName),
                                org.apache.hadoop.fs.Options.Rename.OVERWRITE);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    "Exception occured" + e.getMessage());
            return false;
        }
    }
}
