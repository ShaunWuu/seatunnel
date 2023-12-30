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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class OrcReadStrategyTest {

    @Test
    public void testOrcRead() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        Assertions.assertNotNull(orcFile);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        orcReadStrategy.init(localConf);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        orcReadStrategy.read(orcFilePath, "", testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Boolean.class);
            Assertions.assertEquals(row.getField(1).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(16).getClass(), SeaTunnelRow.class);
        }
    }

    @Test
    public void testOrcReadProjection() throws Exception {
        URL orcFile = OrcReadStrategyTest.class.getResource("/test.orc");
        URL conf = OrcReadStrategyTest.class.getResource("/test_read_orc.conf");
        Assertions.assertNotNull(orcFile);
        Assertions.assertNotNull(conf);
        String orcFilePath = Paths.get(orcFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        OrcReadStrategy orcReadStrategy = new OrcReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        orcReadStrategy.init(localConf);
        orcReadStrategy.setPluginConfig(pluginConfig);
        TestCollector testCollector = new TestCollector();
        SeaTunnelRowType seaTunnelRowTypeInfo =
                orcReadStrategy.getSeaTunnelRowTypeInfo(orcFilePath);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        orcReadStrategy.read(orcFilePath, "", testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(1).getClass(), Boolean.class);
        }
    }

    public static final String ORIGIN_STRING_0 = "字符串列";
    public static final String ORIGIN_STRING_1 = "可变字符列";
    public static final String ORIGIN_STRING_2 = "字符列";
    public static final String ORIGIN_STRING_3 = "二进制列";

    @Test
    public void testOrcReadEncodedByUtf8() throws Exception {
        testReadWithFileEncoding(
                "/encoding/test_read_orc.conf",
                "/encoding/source_file/test_encoding.orc",
                new OrcReadStrategy(),
                StandardCharsets.UTF_8);
    }

    @Test
    public void testCsvReadEncodedByUtf8() throws Exception {
        testReadWithFileEncoding(
                "/encoding/test_csv_read.conf",
                "/encoding/source_file/test_encoding.csv",
                ReadStrategyFactory.of(FileFormat.CSV.name()),
                StandardCharsets.UTF_8);
    }

    public void testReadWithFileEncoding(
            String confFilePath, String filePath, ReadStrategy readStrategy, Charset charset)
            throws Exception {
        // charset of source orc file is UTF-8.
        URL sourceFile = OrcReadStrategyTest.class.getResource(filePath);
        URL conf = OrcReadStrategyTest.class.getResource(confFilePath);
        Assertions.assertNotNull(sourceFile);
        Assertions.assertNotNull(conf);
        String sourceFilePath = Paths.get(sourceFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        readStrategy.init(localConf);
        readStrategy.setPluginConfig(pluginConfig);
        readStrategy.getFileNamesByPath(sourceFilePath);
        TestCollector testCollector = new TestCollector();
        // FIXME: TextReadStrategy#getSeaTunnelRowTypeInfo 会使用默认的 content 作为 key
        SeaTunnelRowType seaTunnelRowTypeInfo =
                CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        readStrategy.setSeaTunnelRowTypeInfo(seaTunnelRowTypeInfo);
        readStrategy.read(sourceFilePath, "", testCollector);
        for (SeaTunnelRow row : testCollector.getRows()) {
            String field0 = (String) row.getField(0);
            String field1 = (String) row.getField(1);
            String field2 = (String) row.getField(2);
            String field3 = new String((byte[]) row.getField(3), StandardCharsets.UTF_8);
            Assertions.assertTrue(isEncoding(field0, ORIGIN_STRING_0, charset));
            log.error(field0);
            log.error(field1);
            log.error(field2);
            log.error(field3);
            Assertions.assertTrue(isEncoding(field1, ORIGIN_STRING_1, charset));
            Assertions.assertTrue(isEncoding(field2, ORIGIN_STRING_2, charset));
            Assertions.assertTrue(isEncoding(field3, ORIGIN_STRING_3, charset));
        }
    }

    private static boolean isEncoding(String checkString, String originString, Charset charset) {
        byte[] originStringBytes = originString.getBytes(StandardCharsets.UTF_8);
        return checkString.equals(new String(originStringBytes, charset));
    }

    public static class TestCollector implements Collector<SeaTunnelRow> {

        private final List<SeaTunnelRow> rows = new ArrayList<>();

        public List<SeaTunnelRow> getRows() {
            return rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            log.info(record.toString());
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
