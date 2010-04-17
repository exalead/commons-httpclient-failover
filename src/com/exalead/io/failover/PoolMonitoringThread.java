/* Copyright 2010 Exalead S.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. 
 */

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
    int loopDelay = 500;

    public void run() {
        Thread.currentThread().setName("PoolMonitoring-" + Thread.currentThread().getId());
        while (!stop) {
            monitorLoop();
            try {Thread.sleep(loopDelay);} catch (InterruptedException e) {}
        }
    }

    /** 
     * Each loop of the monitoring thread checks one specific host using one
     * specific connection
     */
    public void monitorLoop() {
        HostState host = null;
        MonitoredConnection c = null;

        synchronized(pool) {
            host = pool.nextToMonitor();
            NDC.push("monitor:" + host.getURI());
            if (logger.isDebugEnabled()) {
                logger.debug("Start monitoring loop: "+ host);
            }

            /* We'll monitor this host using the connection that hasn't been checked for 
             * the most time
             */
            c = host.getOldestCheckedConnection();
            if (c != null) {
                host.removeFreeConnection(c);
            }
        }

        /* There is no current connection for this host, so we need to connect */
        if (c == null) {
            try {
                logger.debug("connect to host");
                c = host.connect(pool.connectionTimeout);
            } catch (IOException e) {
                synchronized(pool) {
                    logger.info("Connection failed: " + e.getMessage());
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
            boolean ret =  pool.checkConnection(c);
            if (ret == false) {
                logger.info("Host is not alive: " + host);
                /* Host is up but not alive: just kill all connections.
                 * It's useless to try another connection: host knows it's not alive
                 */
                synchronized(pool) {
                    host.down = true;
                    host.killAllConnections();
                }
            } else {
                if (host.down) {
                    logger.info("Host is alive: " + host);
                } else {
                    if (logger.isDebugEnabled()) logger.debug("Host is alive: " + host);
                }
                /* Everything OK */
                synchronized(pool) {
                    host.down = false;
                    c.lastMonitoringTime = System.currentTimeMillis();
                    host.addFreeConnection(c);
                }
            }
        } catch (IOException e) {
            synchronized(pool) {
                if (e instanceof SocketTimeoutException) {
                    logger.info("Host isAlive check timeout: " + host);

                    /* Timeout while trying to get isAlive -> host is hanged.
                     * Checking all connections could be too costly -> kill all connections.
                     * We'll retry later
                     */
                    host.down = true;
                    host.killAllConnections();
                    /* Don't forget to close this connection to avoid FD leak */
                    c.conn.close();
                } else {
                    logger.info("Host isAlive check failure:"  + host);
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
        /* Perform auto scale-down */
        synchronized(pool) {
            if (pool.autoScaleIdleConnections) {
                if (++host.usedConnectionsInPastIdx == host.usedConnectionsTS) {
                    host.usedConnectionsInPastIdx = 0;
                }

                /* Don't do the pruning each time, only once per loop */
                if (host.usedConnectionsInPastIdx == 0) {
                    int max = 0;
                    for (int i = 0; i < host.usedConnectionsTS; i++) {
                        if (host.usedConnectionsInPast[i] > max) max = host.usedConnectionsInPast[i];
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Max = " + max +" cur =" + host.usedConnections + " free=" + host.freeConnections.size());
                    }
                    if (host.freeConnections.size() > max + 1) {
                        logger.info("Closing "  + (host.freeConnections.size() - max - 1) + " connections");
                        for (int i = 0; i < host.freeConnections.size() - max - 1; i++) {
                            MonitoredConnection mc = host.freeConnections.remove(host.freeConnections.size() - 1);
                            mc.conn.close();
                        }
                    }
                }
                host.usedConnectionsInPast[host.usedConnectionsInPastIdx] = host.usedConnections;
            }
        }

        NDC.pop();
        /* End check, end monitoringLoop */
    }

    private static Logger logger = Logger.getLogger("httpclient.failover");
}
