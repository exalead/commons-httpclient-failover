package com.exalead.io.failover;

import java.io.IOException;

import org.apache.commons.httpclient.ConnectionPoolTimeoutException;

/**
 * Work-around a problem with HttpClient:
 * a connectionManager can only throw "ConnectionPoolTimeoutException" when acquiring a connection.
 * So if have a problem, we throw this fake exception
 */
public class FailureFakeConnectionPoolTimeoutException extends ConnectionPoolTimeoutException {
    public FailureFakeConnectionPoolTimeoutException(IOException e) {
        super("", e);
    }
}
