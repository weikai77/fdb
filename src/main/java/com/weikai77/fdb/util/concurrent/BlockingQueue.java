package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * 
 * @author kwei
 *
 */
public interface BlockingQueue extends Queue
{
  void put(byte[] itemValue) throws InterruptedException;
  boolean put(byte[] itemValue, long timeout, TimeUnit unit) throws InterruptedException;
  byte[] take() throws InterruptedException;
  byte[] take(long timeout, TimeUnit unit) throws InterruptedException;
}
