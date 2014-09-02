package com.weikai77.fdb.util.concurrent;

/**
 * 
 * @author kwei
 *
 */
public interface Queue
{
  String getId();
  long size();
  long capacity();
  boolean isEmpty();
  boolean isFull();
  boolean offer(byte[] item);
  byte[] poll();
}
