package com.clearstorydata.hive;

import org.apache.hive.service.server.HiveServer2;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Created by Ian1 on 1/24/15.
 */
public class HiveServer2Runner {
    private final static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    private final int port;
    private final String connectionUrl;

    public HiveServer2Runner(int port) {
        this.port = port;
        this.connectionUrl = "jdbc:hive2://localhost:" + port + "/default";
    }

    public void start() {
        try {
            Class.forName(DRIVER_NAME);
            File hadoopDir = new File("/tmp/hiveserver2");
            System.out.println("using hadoopDir : " + hadoopDir);
            final StandaloneHiveServerContext ctx = new StandaloneHiveServerContext(hadoopDir, port);
            HiveServer2 hs = null;
            hs = new HiveServer2();
            hs.init(ctx.getHiveConf());
            hs.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void execute(String ddl) throws Exception {
        Connection con = DriverManager.getConnection(connectionUrl, "hive1", "");
        Statement stmt = con.createStatement();
        try {
            System.out.println("Running: " + ddl);
            stmt.execute(ddl);
        } finally {
            stmt.close();
            con.close();
        }
    }

}
