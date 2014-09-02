package com.weikai77.fdb.util.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.weikai77.fdb.util.concurrent.DistributedGroup.GroupMembershipListener;
import com.weikai77.util.ConsistentKeyMapper;
import com.weikai77.util.KeyMapper;
import com.weikai77.util.XXHasher;

/**
 * 
 * @author kwei
 *
 */
public class GroupKeyMapper extends GroupMembershipListener implements KeyMapper<String, String>
{
  private static final Logger LOGGER = LoggerFactory.getLogger(GroupKeyMapper.class);

  private final DistributedGroup _group;
  private final int _replicationFactor;
  private final AtomicReference<ConsistentKeyMapper<String,String>> _mapperHolder;
  private final long _pollingIntervalMillis;
  private final ScheduledExecutorService _timer;
  private volatile ScheduledFuture<?> _timerTask;
  
  public GroupKeyMapper(DistributedGroup group, int replicationFactor, long pollingIntervalMillis,
      ScheduledExecutorService timer)
  {
    _group = group;
    _replicationFactor = replicationFactor;
    _mapperHolder = new AtomicReference<>();
    _pollingIntervalMillis = pollingIntervalMillis;
    _timer = timer;
  }

  public GroupKeyMapper(DistributedGroup group, int replicationFactor, long pollingIntervalMillis)
  {
    this(group, replicationFactor, pollingIntervalMillis, Executors.newSingleThreadScheduledExecutor());
  }

  public void start()
  {
    _group.registerMembershipListener(this);
    refresh();
    
    // need to poll periodically to catch expired group members
    _timerTask = _timer.scheduleWithFixedDelay(new Runnable()
    {
      @Override
      public void run()
      {
        refresh();
      }
    }, _pollingIntervalMillis, _pollingIntervalMillis, TimeUnit.MILLISECONDS);
  }
  
  public void shutdown()
  {
    _group.unregisterMembershipListener(this);
    
    if (_timerTask != null)
    {
      _timerTask.cancel(true);
    }
  }
  
  private void refresh()
  {
    LOGGER.info("Refreshing GroupKeyMapper for group " + _group.getId());
    ConsistentKeyMapper<String,String> mapper = new ConsistentKeyMapper<>(_group.listMembers(), 
        XXHasher.getInstance(), XXHasher.getInstance(), _replicationFactor);
    _mapperHolder.set(mapper);
  }

  @Override
  public String map(String key)
  {
    return _mapperHolder.get().map(key);
  }
  
  public int size()
  {
    return _mapperHolder.get().size();
  }

  @Override
  protected void onMembershipChange()
  {
    refresh();
  }

}
