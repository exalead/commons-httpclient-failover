package com.exalead.io.failover;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

/**
 * @file
 * This class is largely derived of the MonitoredHttpConnectionManager.
 * Copyright the Apache Software Foundation.
 * Licensed under the Apache License 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */


public class PoolMonitoringThread extends Thread {
    MonitoredHttpConnectionManager pool;
    volatile boolean stop;
    int loopDelay = 1000;
    
    public void run() {
        Thread.currentThread().setName("PoolMonitoring-" + Thread.currentThread().getId());
        while (!stop) {
            monitorLoop();
            try {Thread.sleep(loopDelay);} catch (InterruptedException e) {}
        }
    }

    public void monitorLoop() {
        HostState host = null;
        MonitoredConnection c = null;
        
        logger.info("******** Start monitoring loop");
        synchronized(pool) {
            host = pool.nextToMonitor();
            NDC.push("monitor:" + host.getURI());
            /* We'll monitor this host using the connection that hasn't been checked for 
             * the most time
             */
            c = host.getOldestCheckedConnection();
            if (c != null) {
                host.removeFreeConnection(c);
            }
            logger.info("Start monitoring loop: monitor " + host + " have connection = " + (c != null));
        }

        /* There is no current connection for this host, so we need to connect */
        if (c == null) {
            try {
                logger.info("[Monitoring] Connect");
                c = host.connect(pool.connectionTimeout);
            } catch (IOException e) {
                synchronized(pool) {
                    logger.info("[Monitoring] Connection failed: " + e.getMessage());
                    
                    /* Same logic than in pool.acquire. See comment there. */
                    host.down = true;
                    host.killAllConnections();
                }
                NDC.pop();
                return;
            }
        }
        
        if (c == null) throw new Error("Error, null connection");

        try {
            logger.info("[Monitoring] Check connection");
            boolean ret =  pool.checkConnection(c);
            if (ret == false) {
                logger.info("[Monitoring] Host NOT alive");
                /* Host is up but not alive: just kill all connections.
                 * It's useless to try another connection: host knows it's not alive
                 */
                synchronized(pool) {
                    host.down = true;
                    host.killAllConnections();
                }
            } else {
                logger.info("[Monitoring] Host alive");
                /* Everything OK */
                host.down = false;
                c.lastMonitoringTime = System.currentTimeMillis();
                host.addFreeConnection(c);
            }
        } catch (IOException e) {
            synchronized(pool) {
                if (e instanceof SocketTimeoutException) {
                    logger.info("[Monitoring] Host timeout");
                    
                    /* Timeout while trying to get isAlive -> host is hanged.
                     * Checking all connections could be too costly -> kill all connections.
                     * We'll retry later
                     */
                    host.down = true;
                    host.killAllConnections();
                    /* Don't forget to close this connection to avoid FD leak */
                    c.conn.close();
                } else {
                    logger.info("[Monitoring] Host RST");
                    /* Connection failure. Server looks down (connection reset by peer). But it could
                     * be only that connection which failed (TCP timeout for example). In that case, 
                     * we just try to fast-kill the stale connections. 
                     * TODO : Avoid doing this with the lock
                     */
                    host.down = true;
                    host.killStaleConnections();
                    /* Don't forget to close this connection to avoid FD leak */
                    c.conn.close();
                }
            }
        }
        NDC.pop();
        /* End check, end monitoringLoop */
    }

    private static Logger logger = Logger.getLogger("httpclient.failover" +
    		"" +
    		"");
}
