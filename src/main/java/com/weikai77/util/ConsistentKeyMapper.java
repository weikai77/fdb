package com.weikai77.util;

import java.util.Collection;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 
 * @author kwei
 *
 */
public class ConsistentKeyMapper<K,N> implements KeyMapper<K, N> 
{
  private final SortedMap<Integer,N> _ring;
  private final Hasher<K> _keyHasher;
  private final Hasher<N> _nodeHasher;
  private final int _replicationFactor;

  /**
   * @param nodes nodes to be placed on the hashing ring
   * @param hasher the hash function to use
   * @param replicationFactor number of virtual nodes each node is assigned to
   */
  public ConsistentKeyMapper(Collection<N> nodes, Hasher<K> keyHasher, Hasher<N> nodeHasher, 
      int replicationFactor)
  {
    _ring = Collections.synchronizedSortedMap(new TreeMap<>());
    _keyHasher = keyHasher;
    _nodeHasher = nodeHasher;
    _replicationFactor = replicationFactor;
    
    for (N node : nodes)
    {
      add(node);
    }
  }
  
  public void add(N node)
  {
    for (int i=0; i<_replicationFactor; i++)
    {
      _ring.put(_nodeHasher.hash(node, i), node);
    }
  }
  
  public void remove(N node)
  {
    for (int i=0; i<_replicationFactor; i++)
    {
      _ring.remove(_nodeHasher.hash(node, i));
    }
  }

  @Override
  public N map(K key)
  {
    if (_ring.isEmpty())
    {
      return null;
    }

    int keyHash = _keyHasher.hash(key, 0);

    // unlikely, but possible
    N node = _ring.get(keyHash);
    if (node != null)
    {
      return node;
    }

    // find the neighboring node on the ring clock-wise
    SortedMap<Integer,N> tailMap = _ring.tailMap(keyHash);
    Integer nextKey = tailMap.isEmpty() ? _ring.firstKey() : tailMap.firstKey();
    return _ring.get(nextKey);
  }
  
  public int size()
  {
    return _ring.size();
  }

}
