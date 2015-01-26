package com.clearstorydata;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

/**
 * Created by Ian1 on 12/23/14.
 */
public class Hive2JdbcTest {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }
        //replace "hive" here with the name of the user the queries should run as
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10003/default", "hive", "");
        //Connection con = DriverManager.getConnection("jdbc:hive2://10.0.0.83:9999/default", "hive", "hive");
        Statement stmt = con.createStatement();

        // regular hive query
        String sql = "select * from csd_agent_tests.csd_agent_acceptance_test_table";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
