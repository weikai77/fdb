package com.weikai77.util;

/**
 * 
 * @author kwei
 *
 */
public interface Hasher<K>
{
  int hash(K key, int seed);
}
