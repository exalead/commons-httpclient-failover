package com.exalead.io.failover;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.ConnectTimeoutException;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.util.IdleConnectionHandler;
import org.apache.log4j.Logger;

import com.sun.xml.internal.ws.api.pipe.NextAction;

/**
 * @file
 * This class is largely derived of the MonitoredHttpConnectionManager.
 * Copyright the Apache Software Foundation.
 * Licensed under the Apache License 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */


public class PoolMonitoringThread extends Thread {
    MonitoredHttpConnectionManager pool;
    
    boolean stop;

    public void run() {
        while (!stop) {
            monitorLoop();
        }
    }

    public void monitorLoop() {
        HostState host = null;
        MonitoredConnection c = null;
        synchronized(pool) {
            host = pool.nextToMonitor();

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
                c = host.connect(pool.connectionTimeout);
            } catch (IOException e) {
                synchronized(pool) {
                    /* Same logic than in pool.acquire. See comment there. */
                    host.down = true;
                    host.killAllConnections();
                }
            }
        }

        try {
            boolean ret =  pool.checkConnection(c);
            pool.checkConnection(c);
            if (ret == false) {
                /* Host is up but not alive: just kill all connections.
                 * It's useless to try another connection: host knows it's not alive
                 */
                synchronized(pool) {
                    host.down = true;
                    host.killAllConnections();
                }
            } else {
                /* Everything OK */
                c.lastMonitoringTime = System.currentTimeMillis();
            }
        } catch (IOException e) {
            synchronized(pool) {
                if (e instanceof SocketTimeoutException) {
                    /* Timeout while trying to get isAlive -> host is hanged.
                     * Checking all connections could be too costly -> kill all connections.
                     * We'll retry later
                     */
                    host.down = true;
                    host.killAllConnections();
                    /* Don't forget to close this connection to avoid FD leak */
                    c.conn.close();
                } else {
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
        /* End check, end monitoringLoop */
    }

}
