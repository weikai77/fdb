package com.weikai77.fdb.util;

import com.foundationdb.subspace.Subspace;

/**
 * 
 * @author kwei
 * 
 */
public interface Space
{
  Space subspace(Object... path);
  Subspace rawSubspace(Object... path);
  byte[] getKey();
}
