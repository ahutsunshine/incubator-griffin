/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package org.apache.griffin.core.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class FSUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(FSUtil.class);

    @Value("${fs.defaultFS}")
    private static String fsDefaultName;

    private static FileSystem fileSystem;

    static {
        try {
            initFileSystem();
        } catch (IOException e) {
            LOGGER.error("cannot get hdfs file system.", e);
        }
    }

    private static void initFileSystem() throws IOException {
        Configuration conf = new Configuration();
        if (!StringUtils.isEmpty(fsDefaultName)) {
            conf.set("fs.defaultFS", fsDefaultName);
            LOGGER.info("Setting fs.defaultFS:{}",fsDefaultName);
        }
        if (StringUtils.isEmpty(conf.get("fs.hdfs.impl"))) {
            LOGGER.info("Setting fs.hdfs.impl:{}",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        }
        if (StringUtils.isEmpty(conf.get("fs.file.impl"))) {
            LOGGER.info("Setting fs.hdfs.impl:{}",org.apache.hadoop.fs.LocalFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }
        fileSystem = FileSystem.get(conf);
    }


    /**
     * list all sub dir of a dir
     *
     * @param dir
     * @return
     * @throws IOException
     */
    public static List<String> listSubDir(String dir) throws IOException {
        if (fileSystem == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        Path path = new Path(dir);
        if (fileSystem.isFile(path)) {
            return new ArrayList<>();
        }

        List<String> fileList = new ArrayList<String>();
        FileStatus[] statuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : statuses) {
            if (fileStatus.isDirectory()) {
                fileList.add(fileStatus.getPath().toString());
            }
        }
        return fileList;

    }

    /**
     * get all file status of a dir.
     *
     * @param dir
     * @return
     * @throws IOException
     */
    public static List<FileStatus> listFileStatus(String dir) throws IOException {
        if (fileSystem == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        Path path = new Path(dir);
        if (fileSystem.isFile(path)) {
            return null;
        }
        List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
        FileStatus[] statuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : statuses) {
            if (!fileStatus.isDirectory()) {
                fileStatusList.add(fileStatus);
            }
        }
        return fileStatusList;
    }

    /**
     * touch file
     *
     * @param filePath
     * @throws IOException
     */
    public static void touch(String filePath) throws IOException {
        if (fileSystem == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        Path path = new Path(filePath);
        FileStatus st;
        if (fileSystem.exists(path)) {
            st = fileSystem.getFileStatus(path);
            if (st.isDirectory()) {
                throw new IOException(filePath + " is a directory");
            } else if (st.getLen() != 0) {
                throw new IOException(filePath + " must be a zero-length file");
            }
        }
        FSDataOutputStream out = null;
        try {
            out = fileSystem.create(path);
        } finally {
            if (out != null) {
                out.close();
            }
        }

    }



    public static boolean isFileExist(String path) throws IOException {
        if (fileSystem == null) {
            throw new NullPointerException("FileSystem is null.Please check your hdfs config default name.");
        }
        Path hdfsPath = new Path(path);
        if (fileSystem.isFile(hdfsPath) || fileSystem.isDirectory(hdfsPath)) {
            return true;
        }
        return false;
    }

}