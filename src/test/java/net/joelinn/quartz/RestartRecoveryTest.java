package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.RedisJobStore;
import net.joelinn.quartz.jobstore.RedisTriggerState;
import org.junit.After;
import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;
import org.quartz.Trigger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Several job stores in one JVM share one Redis, like the nodes of a cluster do. A node which crashed is a store which does
 * not register as alive any more, a node which is stopped properly calls shutdown().
 * What must hold: the triggers of a dead node are taken over soon, the triggers of a live node never are.
 */
public class RestartRecoveryTest extends BaseTest {

    private final List<RedisJobStore> stores = new ArrayList<>();

    @After
    public void shutDownStores() {
        for (RedisJobStore s : stores) {
            try { s.shutdown(); } catch (Exception ignored) { }
        }
    }

    /** a node of the cluster */
    private RedisJobStore node(String id, long clusterCheckin, long releaseInterval, long heartbeat) throws Exception {
        RedisJobStore s = new RedisJobStore();
        s.setHost(host);
        s.setPort(port);
        s.setDatabase(1);
        s.setLockTimeout(5000);
        s.setInstanceId(id);
        s.setClusterCheckinInterval(clusterCheckin);
        s.setReleaseTriggersInterval(releaseInterval);
        s.setHeartbeatInterval(heartbeat);
        s.initialize(null, mockScheduleSignaler);
        stores.add(s);
        return s;
    }

    private OperableTrigger cronTrigger(String name, JobKey job) {
        CronTriggerImpl t = (CronTriggerImpl) TriggerBuilder.newTrigger().withIdentity(name, "g").forJob(job)
                .withSchedule(CronScheduleBuilder.cronSchedule("0 * * * * ?")).build();
        t.computeFirstFireTime(null);
        return t;
    }

    /** stores a job with one trigger, returns the trigger */
    private OperableTrigger store(RedisJobStore on, String name, Class<? extends org.quartz.Job> jobClass) throws Exception {
        JobDetail job = JobBuilder.newJob(jobClass).withIdentity("job-" + name, "g").build();
        OperableTrigger trigger = cronTrigger("trigger-" + name, job.getKey());
        on.storeJobAndTrigger(job, trigger);
        return trigger;
    }

    private static List<OperableTrigger> acquire(RedisJobStore s) throws Exception {
        // everything which is due within the next two minutes
        return s.acquireNextTriggers(System.currentTimeMillis() + 120_000, 10, 0);
    }

    /** acquire and fire, like the scheduler thread does */
    private OperableTrigger acquireAndFire(RedisJobStore s, String name) throws Exception {
        List<OperableTrigger> acquired = acquire(s);
        assertEquals(1, acquired.size());
        assertEquals("trigger-" + name, acquired.get(0).getKey().getName());
        s.triggersFired(acquired);
        return acquired.get(0);
    }

    private Trigger.TriggerState state(RedisJobStore s, OperableTrigger t) throws Exception {
        return s.getTriggerState(t.getKey());
    }

    private String stateSet(OperableTrigger t) {
        String hash = schema.triggerHashKey(t.getKey());
        for (RedisTriggerState s : RedisTriggerState.values()) {
            if (jedis.zscore(schema.triggerStateKey(s), hash) != null) return s.name();
        }
        return "NONE";
    }

    /** milliseconds until the other node gets the trigger, -1 if it does not within the timeout */
    private long millisUntilTaken(RedisJobStore taker, OperableTrigger trigger, long timeoutMs) throws Exception {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            for (OperableTrigger got : acquire(taker)) {
                if (got.getKey().equals(trigger.getKey())) return System.currentTimeMillis() - start;
            }
            Thread.sleep(50);
        }
        return -1;
    }

    // ---- sign of life ---------------------------------------------------------------------------------------------

    @Test
    public void heartbeatKeepsAnIdleSchedulerAlive() throws Exception {
        RedisJobStore a = node("A", 1500, 600_000, 200);
        a.schedulerStarted();
        // nobody asks for work for longer than the check-in interval
        Thread.sleep(2500);
        long last = Long.parseLong(jedis.hget(schema.lastInstanceActiveTime(), "A"));
        assertTrue("last sign of life is " + (System.currentTimeMillis() - last) + " ms old", System.currentTimeMillis() - last < 1000);
    }

    @Test
    public void shutdownUnregistersTheScheduler() throws Exception {
        RedisJobStore a = node("A", 60_000, 600_000, 200);
        a.schedulerStarted();
        assertNotNull(jedis.hget(schema.lastInstanceActiveTime(), "A"));
        a.shutdown();
        assertNull(jedis.hget(schema.lastInstanceActiveTime(), "A"));
        // and nobody beats again after the shutdown
        Thread.sleep(500);
        assertNull(jedis.hget(schema.lastInstanceActiveTime(), "A"));
    }

    @Test
    public void acquiredTriggersAreMarkedWithTheirOwner() throws Exception {
        RedisJobStore a = node("A", 60_000, 600_000, 0);
        OperableTrigger t = store(a, "x", TestJob.class);
        assertEquals(1, acquire(a).size());
        assertEquals("ACQUIRED", stateSet(t));
        assertEquals("A", jedis.get(schema.triggerLockKey(t.getKey())));
    }

    // ---- a node which died ------------------------------------------------------------------------------------------

    @Test
    public void acquiredTriggersOfACrashedNodeAreReleased() throws Exception {
        // A does not beat: after its last action it looks like a crashed node
        RedisJobStore a = node("A", 1000, 200, 0);
        RedisJobStore b = node("B", 1000, 200, 100);
        b.schedulerStarted();
        OperableTrigger t = store(a, "x", TestJob.class);
        assertEquals(1, acquire(a).size());

        long took = millisUntilTaken(b, t, 6000);
        assertTrue("the trigger was never released", took >= 0);
        assertTrue("released after " + took + " ms, A was still alive", took >= 700);
        assertTrue("released only after " + took + " ms", took < 3500);
    }

    @Test
    public void blockedTriggersOfACrashedNodeAreReleased() throws Exception {
        RedisJobStore a = node("A", 1000, 200, 0);
        RedisJobStore b = node("B", 1000, 200, 100);
        b.schedulerStarted();
        OperableTrigger t = store(a, "x", TestJobNonConcurrent.class);
        acquireAndFire(a, "x");                       // A starts the stateful job and dies while it runs
        assertEquals("BLOCKED", stateSet(t));
        assertEquals("A", jedis.get(schema.jobBlockedKey(t.getJobKey())));

        long took = millisUntilTaken(b, t, 6000);
        assertTrue("the trigger was never released", took >= 0);
        assertTrue("released after " + took + " ms, A was still alive", took >= 700);
        assertTrue("released only after " + took + " ms", took < 3500);
    }

    @Test
    public void aRestartedNodeTakesOverWhatTheCrashedNodeLeftAtStartUp() throws Exception {
        RedisJobStore a = node("A", 500, 600_000, 0);
        OperableTrigger acquired = store(a, "acquired", TestJob.class);
        OperableTrigger blocked = store(a, "blocked", TestJobNonConcurrent.class);
        List<OperableTrigger> got = acquire(a);
        assertEquals(2, got.size());
        a.triggersFired(got);
        Thread.sleep(700);                            // A is dead now

        RedisJobStore a2 = node("A2", 500, 600_000, 100);
        a2.schedulerStarted();                        // like a node which starts again

        assertEquals("WAITING", stateSet(acquired));
        assertEquals("WAITING", stateSet(blocked));
    }

    // ---- a node which is alive --------------------------------------------------------------------------------------

    @Test
    public void triggersOfALiveNodeAreNeverReleased() throws Exception {
        RedisJobStore a = node("A", 600, 100, 100);
        RedisJobStore b = node("B", 600, 100, 100);
        a.schedulerStarted();
        b.schedulerStarted();
        OperableTrigger acquired = store(a, "acquired", TestJob.class);
        OperableTrigger blocked = store(a, "blocked", TestJobNonConcurrent.class);
        List<OperableTrigger> got = acquire(a);
        assertEquals(2, got.size());
        // A fires only the stateful one, the other stays acquired
        a.triggersFired(Collections.singletonList(got.get(0).getKey().getName().equals("trigger-blocked") ? got.get(0) : got.get(1)));

        // B looks for work (and releases) for four times the check-in interval
        assertEquals("the trigger of a live node was taken", -1, millisUntilTaken(b, acquired, 2400));
        assertEquals("BLOCKED", stateSet(blocked));
        assertEquals("ACQUIRED", stateSet(acquired));
    }

    @Test
    public void aJobWhichRunsLongerThanTheTriggerLockIsNotReleased() throws Exception {
        RedisJobStore a = node("A", 600, 100, 100);
        RedisJobStore b = node("B", 600, 100, 100);
        a.schedulerStarted();
        b.schedulerStarted();
        OperableTrigger t = store(a, "x", TestJobNonConcurrent.class);
        acquireAndFire(a, "x");
        // the trigger lock expires after ten minutes: the job runs on
        jedis.del(schema.triggerLockKey(t.getKey()));

        assertEquals("the trigger of a running job was released", -1, millisUntilTaken(b, t, 2000));
        assertEquals("BLOCKED", stateSet(t));

        // when the job is done, the trigger can be taken
        a.triggeredJobComplete(t, a.retrieveJob(t.getJobKey()), Trigger.CompletedExecutionInstruction.NOOP);
        assertEquals("WAITING", stateSet(t));
        assertTrue(millisUntilTaken(b, t, 2000) >= 0);
    }

    @Test
    public void anAcquiredTriggerWithoutOwnerMarkerIsOnlyReleasedWhenOverdue() throws Exception {
        // what a node running an older version of the store leaves: acquired, but no trigger lock
        RedisJobStore a = node("A", 600, 100, 100);
        RedisJobStore b = node("B", 600, 100, 100);
        a.schedulerStarted();
        b.schedulerStarted();
        OperableTrigger t = store(a, "x", TestJob.class);
        assertEquals(1, acquire(a).size());
        jedis.del(schema.triggerLockKey(t.getKey()));

        // not due yet: left alone
        assertEquals(-1, millisUntilTaken(b, t, 1000));
        assertEquals("ACQUIRED", stateSet(t));

        // due two minutes ago and still not fired: orphaned
        jedis.zadd(schema.triggerStateKey(RedisTriggerState.ACQUIRED), System.currentTimeMillis() - 120_000, schema.triggerHashKey(t.getKey()));
        assertTrue(millisUntilTaken(b, t, 2000) >= 0);
    }

    // ---- a node which is stopped properly ---------------------------------------------------------------------------

    @Test
    public void whenANodeIsStoppedTheOthersTakeOverAtOnce() throws Exception {
        // long intervals: only the shutdown can make this quick
        RedisJobStore a = node("A", 600_000, 600_000, 100);
        RedisJobStore b = node("B", 600_000, 600_000, 100);
        a.schedulerStarted();
        b.schedulerStarted();
        OperableTrigger acquired = store(a, "acquired", TestJob.class);
        OperableTrigger done = store(a, "done", TestJobNonConcurrent.class);
        // A fired and completed the stateful job and still holds the other trigger
        List<OperableTrigger> got = acquire(a);
        assertEquals(2, got.size());
        OperableTrigger doneTrigger = got.get(0).getKey().getName().equals("trigger-done") ? got.get(0) : got.get(1);
        a.triggersFired(Collections.singletonList(doneTrigger));
        a.triggeredJobComplete(doneTrigger, a.retrieveJob(doneTrigger.getJobKey()), Trigger.CompletedExecutionInstruction.NOOP);

        assertEquals("taken while A is running", -1, millisUntilTaken(b, acquired, 600));

        a.shutdown();

        long took = millisUntilTaken(b, acquired, 2000);
        assertTrue("not released after the shutdown", took >= 0 && took < 1000);
    }

    @Test
    public void aStoppedNodeWithARunningJobStaysRegisteredSoTheJobIsNotStartedTwice() throws Exception {
        // shutdown without waiting for the jobs: the stateful job is still running
        RedisJobStore a = node("A", 600_000, 100, 100);
        RedisJobStore b = node("B", 600_000, 100, 100);
        a.schedulerStarted();
        b.schedulerStarted();
        OperableTrigger t = store(a, "x", TestJobNonConcurrent.class);
        acquireAndFire(a, "x");

        a.shutdown();

        assertNotNull("unregistered although a job is running", jedis.hget(schema.lastInstanceActiveTime(), "A"));
        assertEquals("the running job was started a second time", -1, millisUntilTaken(b, t, 1500));
        assertEquals("BLOCKED", stateSet(t));
    }

    // ---- state changes are atomic -----------------------------------------------------------------------------------

    private static final String NO_STATE_SCRIPT =
            "local sets = {'waiting_triggers','acquired_triggers','blocked_triggers','paused_triggers','paused_blocked_triggers','completed_triggers','error_triggers'} " +
            "local n = 0 " +
            "for _, t in ipairs(redis.call('SMEMBERS', 'triggers')) do " +
            "  local found = false " +
            "  for _, s in ipairs(sets) do if redis.call('ZSCORE', s, t) then found = true break end end " +
            "  if not found then n = n + 1 end " +
            "end " +
            "return n";

    @Test
    public void aTriggerIsNeverInNoStateSet() throws Exception {
        RedisJobStore a = node("A", 60_000, 600_000, 0);
        List<OperableTrigger> triggers = new ArrayList<>();
        for (int i = 0; i < 5; i++) triggers.add(store(a, "t" + i, TestJobNonConcurrent.class));

        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong changes = new AtomicLong();
        final Throwable[] failure = new Throwable[1];
        Thread writer = new Thread(() -> {
            try {
                while (run.get()) {
                    for (OperableTrigger t : triggers) {
                        a.pauseTrigger(t.getKey());
                        a.resumeTrigger(t.getKey());
                        changes.addAndGet(2);
                    }
                }
            } catch (Throwable t) { failure[0] = t; }
        });
        writer.start();
        long end = System.currentTimeMillis() + 2500;
        long seen = 0, checks = 0;
        while (System.currentTimeMillis() < end) {
            seen += (Long) jedis.eval(NO_STATE_SCRIPT, 0);
            checks++;
        }
        run.set(false);
        writer.join();
        if (failure[0] != null) throw new AssertionError("writer failed", failure[0]);
        assertTrue("too few state changes: " + changes, changes.get() > 100);
        assertEquals("triggers seen in no state set in " + checks + " checks during " + changes + " state changes", 0, seen);
    }

    @Test
    public void aFiredStatefulJobNeverLeavesItsTriggerInNoStateSet() throws Exception {
        // the same for the path the scheduler takes: acquire, fire (waiting, then blocked), complete
        RedisJobStore a = node("A", 60_000, 600_000, 0);
        OperableTrigger t = store(a, "x", TestJobNonConcurrent.class);
        JobDetail job = a.retrieveJob(t.getJobKey());

        final AtomicBoolean run = new AtomicBoolean(true);
        final Throwable[] failure = new Throwable[1];
        final AtomicLong cycles = new AtomicLong();
        Thread worker = new Thread(() -> {
            // its own connection: a Jedis connection must not be shared between threads
            try (redis.clients.jedis.Jedis own = new redis.clients.jedis.Jedis(host, port)) {
                own.select(1);
                while (run.get()) {
                    // make it due again, then run the cycle
                    own.zadd(schema.triggerStateKey(RedisTriggerState.WAITING), System.currentTimeMillis() - 1000, schema.triggerHashKey(t.getKey()));
                    List<OperableTrigger> got = a.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0);
                    if (got.isEmpty()) continue;
                    a.triggersFired(got);
                    a.triggeredJobComplete(got.get(0), job, Trigger.CompletedExecutionInstruction.NOOP);
                    cycles.incrementAndGet();
                }
            } catch (Throwable e) { failure[0] = e; }
        });
        worker.start();
        long end = System.currentTimeMillis() + 2500;
        long seen = 0;
        while (System.currentTimeMillis() < end) seen += (Long) jedis.eval(NO_STATE_SCRIPT, 0);
        run.set(false);
        worker.join();
        if (failure[0] != null) throw new AssertionError("worker failed", failure[0]);
        assertTrue("too few cycles: " + cycles, cycles.get() > 20);
        assertEquals("trigger seen in no state set during " + cycles + " fire cycles", 0, seen);
        assertFalse(a.getTriggerState(t.getKey()) == Trigger.TriggerState.NONE);
    }
}
