package com.weikai77.fdb.util.concurrent;

import java.util.List;

/**
 * 
 * @author kwei
 *
 */
public interface DistributedGroupFactory
{
  List<DistributedGroup> listGroups();
  DistributedGroup createGroup(String id);
  DistributedGroup createGroup(String id, long ttl);
  DistributedGroup getGroup(String id);
  void deleteGroup(String id);
}
