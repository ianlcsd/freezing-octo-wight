package com.clearstorydata.hive;

import org.apache.hive.service.server.HiveServer2;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Ian1 on 1/24/15.
 */
public class HiveServer2Runner {
    private final static CountDownLatch latch = new CountDownLatch(1);
    private final static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main (String[] args) {

        TemporaryFolder tf = new TemporaryFolder(new File("/tmp"));;

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("received shutdown event");
                latch.countDown();
            }
        }));


        HiveServer2 hs = null;
        try {
            Class.forName(driverName);

            tf.create();
            StandaloneHiveServerContext ctx = new StandaloneHiveServerContext(tf);

            hs = new HiveServer2();
            hs.init(ctx.getHiveConf());
            hs.start();

            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }




        System.out.println("shutting down hiveServer2 instance " + hs);
        if ( hs != null) {
            hs.stop();
        }
        System.out.printf("hiveServer2 instance " + hs + "was shut down!");
    }

}
