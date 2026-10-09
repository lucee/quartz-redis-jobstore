# Changelog
### 2.0.0.0-RC (Lucee fork, restart recovery)
Recovery of the triggers of a scheduler which stopped or died, and atomic state changes.
* **Heartbeat**: every scheduler registers as alive from a separate thread (`heartbeatInterval`, default 5 s, 0 turns it off), no longer only when it looks for work. The scheduling loop can be blocked for a long time (all worker threads busy) without the scheduler being taken for dead, so `clusterCheckinInterval` can be much shorter than the default 4 minutes (recommended: at least three times the heartbeat interval, for example 30000).
* **`releaseTriggersInterval`** (default 10 minutes, as before): how often the triggers of dead schedulers are released. With a short `clusterCheckinInterval` set it to a similar value (for example 15000) to recover in well under a minute instead of 5 to 10 minutes.
* **Clean stop**: on shutdown a scheduler unregisters itself and asks the others to release what it still owns right away. Not done while it still owns a running job (shutdown without waiting for the jobs), then it expires like a dead scheduler.
* **Start**: a scheduler which starts takes over the triggers of dead schedulers first.
* **Trigger state changes are atomic** (MULTI/EXEC, single Redis node): a trigger is never in no state set. Before, a scheduler which stopped between "remove" and "add" left the trigger in no state set, where nothing found it again. Blocked and acquired triggers are marked with their owner in the same step.
* **Every acquired trigger is marked with its owner** (before: only triggers of jobs without concurrent execution). An acquired trigger without a marker (acquired by a scheduler running an older version) is only released when it is overdue by more than 60 s.
* A blocked trigger is not released while the scheduler which blocked its job is alive, also when the trigger lock (10 minutes) has expired, so a job which runs longer than that is not started a second time.
* **Fewer round trips to Redis** (one job run, acquire + fire + complete, measured against a Redis with network latency, in round trips): concurrent job 34 -> 17, stateful job 41 -> 19; stateful fire 17 -> 5, idle poll 8 -> 4. With a short network latency this is what limits how many jobs a cluster can start per second, because most of it happens inside the global lock.
  * releasing the global lock is one request (Lua compare-and-delete) instead of two
  * a trigger state change is one request (MULTI/EXEC inside a pipeline) instead of two
  * the heartbeat thread replaces the activity write and the self check of every acquire; whether releases are due is asked from Redis at most every half second
  * `triggersFired` and `triggeredJobComplete` read in one request and write in one request, and no longer do the same writes twice; a fired stateful trigger goes from ACQUIRED straight to BLOCKED (before: via WAITING, where another scheduler could acquire it)
  * new setting `testOnBorrow` (default true, as before): `false` skips the PING when a connection is taken from the pool, saves one more round trip per operation; a connection which broke since it was used last then makes the next operation fail once
  * benchmark: `PerformanceBenchmarkTest` (`-Dbenchmark=true`)
* Redis Cluster mode keeps the old, non atomic state changes (keys in different slots).
* Do not mix with schedulers running 1.x with a short `clusterCheckinInterval`: they do not send a heartbeat.

### 1.2.0.1-RC (Lucee fork, [LDEV-6531](https://luceeserver.atlassian.net/browse/LDEV-6531))
* `retrieveTrigger` and `retrieveJob` read the hash and the data map in one round trip (pipelined)
* New `RedisJobStore.getAllTriggerDetails()`: all triggers with job and state in a few pipelined requests, independent of the number of triggers (takes the global lock once, so the result is a consistent snapshot)
* Implement `getAcquireRetryDelay` and `resetTriggerFromErrorState` (added to the `JobStore` interface in Quartz 2.3)
* Build against Quartz 2.3.2 and Java 11; runtime dependencies unchanged (Jedis 3.3.0, Jackson 2.11.1)
* Tests run against a real Redis (docker or `REDIS_SERVER`) instead of embedded-redis; the embedded-redis based sentinel test was removed
* Published as `org.lucee:quartz-redis-jobstore`

### 2019-07-02
* Upgrade to Jedis 3.0.1

### 2019-06-26
* Delete job data map set from Redis prior to storing new job data when updating / overwriting a job. 
This will prevent keys which were removed from the job's data map prior to storage from being preserved.

### 2018-03-01
* Detect dead schedulers and unblock their blocked triggers
* Keep track of `previousFireTime` for triggers

### 2018-02-27
* Set fire instance id on retrieved triggers
* Fixed a bug where trigger locks would get incorrectly removed for non-concurrent jobs

### 2016-12-30
* Fix a bug when handling trigger firing for triggers with no next fire time

### 2016-12-04
* Fixed handling of jobs marked with `@DisallowConcurrentExecution`.

### 2016-10-30
* Fix serialization of HolidayCalendar

### 2016-10-23
* Add support for storing trigger-specific job data

### 2016-05-04
* Add support for Redis password

### 2016-03-17
* Allow Redis db to be set when using Sentinel

### 2016-03-02
* Fix a bug where acquired triggers were not being released.

### 2016-01-31
* Add support for Redis Sentinel

### 2015-08-19
* Add support for [Jedis cluster](https://github.com/xetorthio/jedis#jedis-cluster).
* Allow a pre-configured Pool<Jedis> or JedisCluster to be passed in to RedisJobStore.
* Update to Jackson v2.6.1.

### 2014-12-09
* Remove Guava dependency

### 2014-09-24
* Add the ability to specify a redis database.
* Fix setter methods for `keyPrefix` and `keyDelimiter` properties.
* Set default port to 6379.

### 2014-08-21
* Fix a bug where non-durable jobs with only one trigger would be deleted when replaceTrigger() was called with that trigger.

### 2014-07-25
* Handle `ObjectAlreadyExistsException` separately in RedisJobStore::storeJobAndTrigger()

### 2014-07-24
* Enable the use of all GroupMatchers (not just EQUALS)