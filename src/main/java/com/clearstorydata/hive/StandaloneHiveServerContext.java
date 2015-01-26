/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.clearstorydata.hive;

import org.apache.hadoop.hive.conf.HiveConf;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

//import org.junit.rules.TemporaryFolder;

/**
 * Configuration for running the HiveServer within this JVM with zero external dependencies.
 * <p/>
 * This class contains a bunch of methods meant to be overridden in order to create slightly different contexts.
 */
public class StandaloneHiveServerContext {

    private String metaStorageUrl;

    private HiveConf hiveConf = new HiveConf();

    private File basedir;

    public StandaloneHiveServerContext(File basedir, int port) throws IOException {
        // Ian's customization
        hiveConf.set("hive.server2.thrift.port", String.valueOf(port));
        hiveConf.set("hive.server2.enable.doAs", "false");
        hiveConf.set("mapred.job.tracker", "local");
        hiveConf.set("mapreduce.framework.name", "local");
        hiveConf.set("mapreduce.job.reduces", "1");
        hiveConf.set("hive.exec.mode.local.auto", "false");

        basedir.deleteOnExit();
        this.basedir = basedir;
        this.metaStorageUrl =  "jdbc:hsqldb:mem:" + UUID.randomUUID().toString() + ";create=true";

        //configureJavaSecurityRealm(HiveConf
        //System.setProperty("java.security.krb5.realm", "");
        //System.setProperty("java.security.krb5.kdc", "");


        hiveConf.set("hive.stats.autogather", "false");
        // Set the hsqldb driver
        hiveConf.set("datanucleus.connectiondrivername", "org.hsqldb.jdbc.JDBCDriver");
        hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.hsqldb.jdbc.JDBCDriver");

        // No pooling needed. This will save us a lot of threads
        hiveConf.set("datanucleus.connectionPoolingType", "None");

        // Defaults to a 1000 millis sleep in
        // org.apache.hadoop.hive.ql.exec.mr.HadoopJobExecHelper.
        hiveConf.set("hive.exec.counters.pull.interval", "1");

        hiveConf.set("hadoop.bin.path", "NO_BIN!");

        /* configureJobTrackerMode
        ReflectionUtils.setStaticField(ShimLoader.class, "hadoopShims", new Hadoop20SShims() {
            @Override
            public boolean isLocalMode(Configuration conf) {
                return false;
            }
        });
        */

        // configureSupportConcurrency
        hiveConf.set("hive.support.concurrency", "false");

        // configureFileSystem
        hiveConf.set("javax.jdo.option.ConnectionURL", metaStorageUrl);
        hiveConf.set("hive.warehouse.subdir.inherit.perms", "true");

        hiveConf.set("hive.metastore.warehouse.dir", basedir.getAbsolutePath() + File.separator + "warehouse");
        hiveConf.set("hive.start.cleanup.scratchdir", basedir.getAbsolutePath() + File.separator + "scratchdir");
        hiveConf.set("hive.exec.local.scratchdir", basedir.getAbsolutePath() + File.separator + "localscratchdir");
        hiveConf.set("hive.querylog.location", basedir.getAbsolutePath() + File.separator + "tmp");
        hiveConf.set("hadoop.tmp.dir", basedir.getAbsolutePath() + File.separator + "hadooptmp");
        hiveConf.set("test.log.dir", basedir.getAbsolutePath() + File.separator + "logs");
        hiveConf.set("hive.vs", basedir.getAbsolutePath() + File.separator + "vs");

        // configureMetaStoreValidation
        hiveConf.set("datanucleus.validateConstraints", "true");
        hiveConf.set("datanucleus.validateColumns", "true");
        hiveConf.set("datanucleus.validateTables", "true");

        // configureMapReduceOptimizations
        /*
        * Switch off all optimizers otherwise we didn't
        * manage to contain the map reduction within this JVM.
        */
        hiveConf.set("hive.exec.infer.bucket.sort", "false");
        hiveConf.set("hive.optimize.metadataonly", "false");
        hiveConf.set("hive.optimize.index.filter", "false");
        hiveConf.set("hive.auto.convert.join", "false");
        hiveConf.set("hive.optimize.skewjoin", "false");


        // configureCheckForDefaultDb
        hiveConf.set("hive.metastore.checkForDefaultDb", "true");

        //  configureAssertionStatus
        //ClassLoader.getSystemClassLoader().setPackageAssertionStatus("org.apache.hadoop.hive.serde2.objectinspector", false);

    }


    private File newFolder(File basedir, String folder) {
        File f = new File(basedir, folder);
        //f.setWritable(true, false);
        return f;
    }
/*
    private File newFile(File basedir, String fileName) {
        File f = new File(basedir, fileName);
        //f.setWritable(true, false);
        return f;
    }
*/

    public HiveConf getHiveConf() {
        return hiveConf;
    }
/*
    protected final void createAndSetFolderProperty(HiveConf.ConfVars var, String folder,
                                                    File basedir) {
        hiveConf.setVar(var, newFolder(basedir, folder).getAbsolutePath());
    }
*/
    protected final void createAndSetFolderProperty(String key, String folder) {
        //hiveConf.set(key, new File(folder).getAbsolutePath());
        hiveConf.set(key, folder);
    }


}
