package com.exalead.io.failover;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.ListIterator;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpConnection;
import org.apache.log4j.Logger;

import com.exalead.io.failover.MonitoredConnection;

/** The state and connections of a host in a failover pool */
public class HostState {
    int power;
    boolean down;
    HostConfiguration configuration;
    LinkedList<MonitoredConnection> freeConnections = new LinkedList<MonitoredConnection>();
    int inFlightConnections;

    /** Remove a connection from the free list */
    void removeFreeConnection(MonitoredConnection c) {
        freeConnections.remove(c);
    }
    
    public String getURI() {
        return configuration.getHostURL();
    }
    
    public String toString() {
        return "[HS: " + getURI() + ",down=" + down + ",fc=" + freeConnections.size() + ",ac=" + inFlightConnections +"]";
    }

    /** Get the connections that were checked after "minDate" */
    List<MonitoredConnection> getRecentlyCheckedConnections(long minDate) {
        List<MonitoredConnection> ret = new ArrayList<MonitoredConnection>();
        for (MonitoredConnection free: freeConnections) {
            if (free.lastMonitoringTime >= minDate) {
                ret.add(free);
            }
        }
        return ret;
    }

    /**
     * Return the connection that was checked least recently.
     * Returns null if there is no currently free connection.
     * Note that this does not remove this connection from the freelist. You must
     * call removeFreeConnection afterwards.
     */
    MonitoredConnection getOldestCheckedConnection() {
        MonitoredConnection c = null;
        long oldestDate = Long.MAX_VALUE;
        for (MonitoredConnection free: freeConnections) {
            if (free.lastMonitoringTime <= oldestDate) {
                oldestDate = free.lastMonitoringTime;
                c = free;
            }
        }
        return c;
    }
    
    MonitoredConnection connect(int timeout) throws IOException {
        MonitoredConnection newConn = new MonitoredConnection();
        newConn.host = this;
        newConn.conn = new HttpConnection(configuration);
        newConn.conn.getParams().setConnectionTimeout(timeout);
        newConn.conn.open();
        newConn.lastMonitoringTime = System.currentTimeMillis();
        return newConn;
    }
    
    /* Close all connections to the host and remove them */
    void killAllConnections() {
        for (MonitoredConnection free: freeConnections) {
            free.conn.close();
        }
        freeConnections.clear();
    }
    
    /* This operation can be a bit long: at most freeConnections.size() milliseconds */
    void killStaleConnections() {
        int closed = 0;
        ListIterator<MonitoredConnection> it = freeConnections.listIterator();
        while (it.hasNext()) {
            MonitoredConnection free = it.next();
            boolean wasStale;
            try {
                wasStale = free.conn.closeIfStale();
            } catch (IOException e) {
                wasStale = true;
                free.conn.close();
            }
            if (wasStale) {
                closed++;
                it.remove();
            }
        }
        logger.info("Closed " + closed + " stale connections, " + freeConnections.size() + " remaining");
    }
    
    void markConnectionsAsUnchecked() {
        for (MonitoredConnection free: freeConnections) {
            free.lastMonitoringTime = 0;
        }
    }
    
    void addFreeConnection(MonitoredConnection mc) {
        freeConnections.add(mc);
    }

    private static Logger logger = Logger.getLogger("httpclient.failover");
}
