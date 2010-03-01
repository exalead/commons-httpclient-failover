package com.exalead.io.failover;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;

public class NoRetryFailoverHttpClient {
    protected MonitoredHttpConnectionManager manager;
    protected HttpClient client;
    
    public NoRetryFailoverHttpClient() {
        manager = new MonitoredHttpConnectionManager();
        client = new HttpClient(manager);
    }
    
    public void addHost(String host, int port, int power) {
        // TODO
    }
    
    public void executeMethod(HttpMethod method) {
        // TODO
    }
}
