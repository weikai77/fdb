package com.weikai77.fdb.util.concurrent;

/**
 * 
 * @author kwei
 *
 */
public interface DistributedConditionFactory
{
  DistributedCondition newCondition(String id);
}
