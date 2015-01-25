package com.clearstorydata;

import com.clearstorydata.hive.test.StandaloneHiveServerContext;
import org.apache.hive.service.server.HiveServer2;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;

/**
 * Created by Ian1 on 12/24/14.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:hive-server.xml"})
public class HiveSpringTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    private HiveServer2 hs;

    @Rule
    public TemporaryFolder tf = new TemporaryFolder(new File("/tmp"));;

    @Before
    public void setup() throws Exception {
        StandaloneHiveServerContext shsc = new StandaloneHiveServerContext(tf, UUID.randomUUID().toString());
        hs = new HiveServer2();
        hs.init(shsc.getHiveConf());
        hs.start();

        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

    }

    @After
    public void teardown() {
        hs.stop();
    }
    @Test
    public void test1() throws Exception {

        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10003/default", "hive1", "");

        //Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10003/default", "hive", "hive");
        //Connection con = DriverManager.getConnection("jdbc:hive2://", "hive", "");

        Statement stmt = con.createStatement();
        String tableName = "testHiveDriverTable";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        /*
        String filepath = "/tmp/a.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);
        */
        // select * query
        sql = "explain select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

        // regular hive query
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }


    @Test
    public void test2() throws Exception {

        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10003/default", "hive1", "");


        Statement stmt = con.createStatement();
        String tableName = "test.boola";
        stmt.execute("create database if not exists test");
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string, desc string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table
        /*
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
        */
        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line

        String filepath = "src/test/resources/csd_agent_acceptance_test_table.csv";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);

/*
        for (int i=1;  i<=100; i++ ) {
            String tmpstmt = String.format("insert into table %s values (%s, %s, %s)", tableName, i, "\"item " + i + "\"", "\"" + i + " squared is " + i*i + "\"");
            System.out.println(tmpstmt);
            stmt.execute(tmpstmt);
        }
*/

         // select * query
        sql = "select * from " + tableName + " where key % 2 = 0 order by key limit 5";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.format("%s, %s, %s", res.getInt(1), res.getString(2), res.getString(3) ));
        }

        // regular hive query
        /*
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
        */
    }

}

