package com.exalead.io.failover;

import org.apache.commons.httpclient.HttpConnection;

public class MonitoredConnection {
    HostState host;
    HttpConnection conn;
    long lastMonitoringTime;
    long lastUseTime;
}
