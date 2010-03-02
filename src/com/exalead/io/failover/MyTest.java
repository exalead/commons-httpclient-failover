package com.exalead.io.failover;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class MyTest {
    private static Logger logger = Logger.getLogger("log");

    FailoverHttpClient relay;

    class MyThread extends Thread {
        public void run() {
            while (true) {
                try {Thread.sleep(30);} catch (InterruptedException e) {}
                
                /*

                GetMethod httpMethod = new GetMethod("/exascript/Ping");
                try {
                    logger.info("********** START method");
                    int retcode = relay.executeMethod(httpMethod);
                    logger.info("********** DONE");
                    InputStream is = httpMethod.getResponseBodyAsStream();
                    is.close();
                } catch (IOException e) {
                    logger.warn("**************** " + System.currentTimeMillis() + ": MAIN EXCEPTION", e);
                }
                
                
                */
                
                boolean success = false;
                for (int i = 0; i < 2; i++) {
                    GetMethod httpMethod = new GetMethod("/exascript/Ping");
                    try {
                        //logger.info("********** START method");
                        relay.executeMethod(httpMethod);
                        //logger.info("********** DONE");
                        InputStream is = httpMethod.getResponseBodyAsStream();
                        is.close();
                        success = true;
                        break;
                    } catch (IOException e) {
                        logger.warn("MAIN EXCEPTION - RETRY");
                        continue;
                    }
                }
                if (!success) {
                    logger.error("****************** RETRY CLIENT COULD NOT RETRY **************");
                }
            } 
        }
    }

    public void run() throws Exception {
        BasicConfigurator.configure();
        Logger.getLogger("org").setLevel(Level.INFO);
        Logger.getLogger("httpclient").setLevel(Level.INFO);
       // Logger.getLogger("org").setLevel(Level.TRACE);

        relay = new FailoverHttpClient();
        relay.setConnectionAcquireTimeout(300);
        relay.addHost("localhost", 31606, 1);
        relay.addHost("localhost", 31616, 1);
        relay.startMonitoring(1);
        
        List<MyThread> threads = new ArrayList<MyThread>();
        for (int i = 0; i < 12; i++) {
            MyThread t = new MyThread();
            t.start();
            threads.add(t);
        }
        for (MyThread mt:  threads) {
            mt.join();
        }
    }

    public static void main(String[] args) throws Exception {
        new MyTest().run();
    }
}
