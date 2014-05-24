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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.SocketException;

import org.apache.commons.httpclient.HttpConnection;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.params.HttpConnectionParams;
import org.apache.commons.httpclient.protocol.Protocol;

/**
 * @file
 * This class is largely derived of the MonitoredHttpConnectionManager.
 * Copyright the Apache Software Foundation.
 * Licensed under the Apache License 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */


/**
 * A helper HttpConnection wrapper that ensures a connection cannot be used
 * once released.
 */
class HttpConnectionAdapter extends HttpConnection {
    // the wrapped connection
    private HttpConnection wrappedConnection;

    /**
     * Creates a new HttpConnectionAdapter.
     * @param connection the connection to be wrapped
     */
    public HttpConnectionAdapter(HttpConnection connection) {
        super(connection.getHost(), connection.getPort(), connection.getProtocol());
        this.wrappedConnection = connection;
    }

    /**
     * Tests if the wrapped connection is still available.
     * @return boolean
     */
    protected boolean hasConnection() {
        return wrappedConnection != null;
    }

    /**
     * @return HttpConnection
     */
    HttpConnection getWrappedConnection() {
        return wrappedConnection;
    }

    public void close() {
        if (hasConnection()) {
            wrappedConnection.close();
        } else {
            // do nothing
        }
    }

    public InetAddress getLocalAddress() {
        if (hasConnection()) {
            return wrappedConnection.getLocalAddress();
        } else {
            return null;
        }
    }

    public void setLocalAddress(InetAddress localAddress) {
        if (hasConnection()) {
            wrappedConnection.setLocalAddress(localAddress);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public String getHost() {
        if (hasConnection()) {
            return wrappedConnection.getHost();
        } else {
            return null;
        }
    }

    public HttpConnectionManager getHttpConnectionManager() {
        if (hasConnection()) {
            return wrappedConnection.getHttpConnectionManager();
        } else {
            return null;
        }
    }

    public InputStream getLastResponseInputStream() {
        if (hasConnection()) {
            return wrappedConnection.getLastResponseInputStream();
        } else {
            return null;
        }
    }

    public int getPort() {
        if (hasConnection()) {
            return wrappedConnection.getPort();
        } else {
            return -1;
        }
    }

    public Protocol getProtocol() {
        if (hasConnection()) {
            return wrappedConnection.getProtocol();
        } else {
            return null;
        }
    }

    public String getProxyHost() {
        if (hasConnection()) {
            return wrappedConnection.getProxyHost();
        } else {
            return null;
        }
    }

    public int getProxyPort() {
        if (hasConnection()) {
            return wrappedConnection.getProxyPort();
        } else {
            return -1;
        }
    }

    public OutputStream getRequestOutputStream()
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            return wrappedConnection.getRequestOutputStream();
        } else {
            return null;
        }
    }

    public InputStream getResponseInputStream()
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            return wrappedConnection.getResponseInputStream();
        } else {
            return null;
        }
    }

    public boolean isOpen() {
        if (hasConnection()) {
            return wrappedConnection.isOpen();
        } else {
            return false;
        }
    }

    public boolean closeIfStale() throws IOException {
        if (hasConnection()) {
            return wrappedConnection.closeIfStale();
        } else {
            return false;
        }
    }

    public boolean isProxied() {
        if (hasConnection()) {
            return wrappedConnection.isProxied();
        } else {
            return false;
        }
    }

    public boolean isResponseAvailable() throws IOException {
        if (hasConnection()) {
            return  wrappedConnection.isResponseAvailable();
        } else {
            return false;
        }
    }

    public boolean isResponseAvailable(int timeout) throws IOException {
        if (hasConnection()) {
            return  wrappedConnection.isResponseAvailable(timeout);
        } else {
            return false;
        }
    }

    public boolean isSecure() {
        if (hasConnection()) {
            return wrappedConnection.isSecure();
        } else {
            return false;
        }
    }

    public boolean isTransparent() {
        if (hasConnection()) {
            return wrappedConnection.isTransparent();
        } else {
            return false;
        }
    }

    public void open() throws IOException {
        if (hasConnection()) {
            wrappedConnection.open();
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public void printLine()
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.printLine();
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public String readLine(String charset) throws IOException, IllegalStateException {
        if (hasConnection()) {
            return wrappedConnection.readLine(charset);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public void releaseConnection() {
        if (!isLocked() && hasConnection()) {
            HttpConnection wrappedConnection = this.wrappedConnection;
            this.wrappedConnection = null;
            wrappedConnection.releaseConnection();
        } else {
            // do nothing
        }
    }

    public void setHost(String host) throws IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.setHost(host);
        } else {
            // do nothing
        }
    }

    public void setHttpConnectionManager(HttpConnectionManager httpConnectionManager) {
        if (hasConnection()) {
            wrappedConnection.setHttpConnectionManager(httpConnectionManager);
        } else {
            // do nothing
        }
    }

    public void setLastResponseInputStream(InputStream inStream) {
        if (hasConnection()) {
            wrappedConnection.setLastResponseInputStream(inStream);
        } else {
            // do nothing
        }
    }

    public void setPort(int port) throws IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.setPort(port);
        } else {
            // do nothing
        }
    }

    public void setProtocol(Protocol protocol) {
        if (hasConnection()) {
            wrappedConnection.setProtocol(protocol);
        } else {
            // do nothing
        }
    }

    public void setProxyHost(String host) throws IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.setProxyHost(host);
        } else {
            // do nothing
        }
    }

    public void setProxyPort(int port) throws IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.setProxyPort(port);
        } else {
            // do nothing
        }
    }

    public void tunnelCreated() throws IllegalStateException, IOException {
        if (hasConnection()) {
            wrappedConnection.tunnelCreated();
        } else {
            // do nothing
        }
    }

    public void write(byte[] data, int offset, int length)
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.write(data, offset, length);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public void write(byte[] data)
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.write(data);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public void writeLine()
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.writeLine();
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public void writeLine(byte[] data)
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.writeLine(data);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public void flushRequestOutputStream() throws IOException {
        if (hasConnection()) {
            wrappedConnection.flushRequestOutputStream();
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public int getSendBufferSize() throws SocketException {
        if (hasConnection()) {
            return wrappedConnection.getSendBufferSize();
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public HttpConnectionParams getParams() {
        if (hasConnection()) {
            return wrappedConnection.getParams();
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    public void setParams(final HttpConnectionParams params) {
        if (hasConnection()) {
            wrappedConnection.setParams(params);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.commons.httpclient.HttpConnection#print(java.lang.String, java.lang.String)
     */
    public void print(String data, String charset) throws IOException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.print(data, charset);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.commons.httpclient.HttpConnection#printLine(java.lang.String, java.lang.String)
     */
    public void printLine(String data, String charset)
    throws IOException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.printLine(data, charset);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.commons.httpclient.HttpConnection#setSocketTimeout(int)
     */
    public void setSocketTimeout(int timeout) throws SocketException, IllegalStateException {
        if (hasConnection()) {
            wrappedConnection.setSocketTimeout(timeout);
        } else {
            throw new IllegalStateException("Connection has been released");
        }
    }
}
