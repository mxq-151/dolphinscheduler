/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.worker.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;

/**
 * Md5Utils
 */
public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {
        throw new IllegalStateException("Utility class");
    }


    // 写入文本文件
    public static void writeToFile(String content, String filename) {
        try {
            FileWriter writer = new FileWriter(filename);
            writer.write(content);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            logger.error("write content to file error: {}", e.getMessage());
        }
    }

    // 从文本文件中读取内容
    public static String readFromFile(String filename) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line = reader.readLine();
            reader.close();
            return line;
        } catch (IOException e) {
            logger.error("read content from file error: {}", e.getMessage());
        }
        return null;
    }

    public static void createSymbolicLink(Path sourcePath, Path destinationPath) throws IOException {

        // 删除目标路径上的文件（如果存在）
        if (Files.exists(destinationPath)) {
            Files.delete(destinationPath);
        }

        // 创建软链接
        Files.createSymbolicLink(destinationPath, sourcePath);

        // 打印日志
        logger.info("Created symbolic link: {} -> {}", destinationPath, sourcePath);
    }


}
