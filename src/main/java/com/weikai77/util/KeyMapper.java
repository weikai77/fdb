package com.weikai77.util;

/**
 * 
 * @author kwei
 *
 * @param <K> type of the key to map on
 * @param <N> type of the node to map the key to
 */
public interface KeyMapper<K,N>
{
  N map(K key);
}
