quartz-redis-jobstore
=====================

[![Build Status](https://secure.travis-ci.org/jlinn/quartz-redis-jobstore.png?branch=master)](http://travis-ci.org/jlinn/quartz-redis-jobstore)

A [Quartz Scheduler](http://quartz-scheduler.org/) JobStore using [Redis](http://redis.io).

This project was inspired by [redis-quartz](https://github.com/RedisLabs/redis-quartz), and provides similar functionality with some key differences:

* Redis database and key prefix are configurable.
* Redis' [recommended distributed locking method](http://redis.io/topics/distlock) is used.
* All of the functionality of this library is covered by a [test suite](https://github.com/jlinn/quartz-redis-jobstore/tree/master/src/test/java/net/joelinn/quartz).

## Requirements
* Java 7 or higher
* Redis 2.6.12 or higher (3.0 or higher for Redis cluster)

## Installation
Maven dependency:
```xml
<dependency>
    <groupId>net.joelinn</groupId>
    <artifactId>quartz-redis-jobstore</artifactId>
    <version>1.2.0</version>
</dependency>
```

## Configuration
The following properties may be set in your `quartz.properties` file:
```
# set the scheduler's JobStore class (required)
org.quartz.jobStore.class = net.joelinn.quartz.jobstore.RedisJobStore

# set the Redis host (required)
org.quartz.jobStore.host = <your redis host or a comma-delimited list of host:port if clustered use is desired>

# set the scheduler's trigger misfire threshold in milliseconds (optional, defaults to 60000)
org.quartz.jobStore.misfireThreshold = 60000

# set the redis password (optional, defaults null)
org.quartz.jobStore.password = <your redis password>

# set the redis port (optional, defaults to 6379)
org.quartz.jobStore.port = <your redis port>

# enable Redis clustering (optional, defaults to false)
org.quartz.jobStore.redisCluster = <true or false> 

# enable Redis sentinel (optional, defaults to false)
org.quartz.jobStore.redisSentinel = <true or false>

# set the sentinel master group name (required if redisSentinel = true)
org.quartz.jobStore.masterGroupName = <your master group name>

# set the redis database (optional, defaults to 0)
org.quartz.jobStore.database: <your redis db>

# set the Redis key prefix for all JobStore Redis keys (optional, defaults to none)
org.quartz.jobStore.keyPrefix = a_prefix_

# set the Redis lock timeout in milliseconds (optional, defaults to 30000)
org.quartz.jobStore.lockTimeout = 30000

# enable SSL (defaults to false)
org.quartz.jobStore.ssl = <true or false>
```

### Recovery of stopped and dead schedulers (2.0)

Every scheduler registers as alive from a separate thread. The triggers a scheduler owns (acquired, or blocked because a job
which must not run concurrently is running) are taken over by the others when it has not been seen for `clusterCheckinInterval`
and a release run happens. Defaults keep the behaviour of 1.x (4 minutes and 10 minutes); for a quick recovery set shorter values:

```
# a scheduler which was not seen for this long is dead (ms, default 240000). At least three times the heartbeat interval
org.quartz.jobStore.clusterCheckinInterval = 30000

# how often the triggers of dead schedulers are released (ms, default 600000)
org.quartz.jobStore.releaseTriggersInterval = 15000

# how often a scheduler tells that it is alive (ms, default 5000, 0 turns the heartbeat off)
org.quartz.jobStore.heartbeatInterval = 5000

# check a connection with a PING when it is taken from the pool (default true). false saves one round trip per operation,
# a connection which broke since it was last used then makes the next operation fail once
org.quartz.jobStore.testOnBorrow = true
```

A job is taken over after at most `clusterCheckinInterval + releaseTriggersInterval` (45 s with the values above, measured:
41 to 48 s; with the defaults 5 to 10 minutes). A scheduler which is shut down without running jobs unregisters, the others
take over at once. Do not set `clusterCheckinInterval` shorter than the longest pause a scheduler can have without
sending the heartbeat (a long garbage collection): its triggers would be taken over while it is still alive.

## Limitations
All GroupMatcher comparators have been implemented. 
Aside from that, the same limitations outlined in [redis-quartz's readme](https://github.com/RedisLabs/redis-quartz#limitations) apply.
