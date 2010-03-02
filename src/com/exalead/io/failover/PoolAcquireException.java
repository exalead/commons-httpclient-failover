package com.exalead.io.failover;

public class PoolAcquireException extends Exception{
  public PoolAcquireException(String message) {
      super(message);
  }
  public PoolAcquireException(String message, Throwable cause) {
      super(message, cause);
  }
}
