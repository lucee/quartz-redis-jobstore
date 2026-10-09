package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.RedisTriggerState;
import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.TriggerFiredResult;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * What is in Redis after a trigger fired and after its job completed, for jobs which may and which may not run
 * concurrently. The exact state is pinned, so the steps can be reorganised (fewer requests) without changing the result.
 */
public class FireCompleteContractTest extends BaseTest {

    private static final String SCHEDULER = "testJobStore1";

    private OperableTrigger cron(String name, JobDetail job) {
        CronTriggerImpl t = (CronTriggerImpl) TriggerBuilder.newTrigger().withIdentity(name, "g").forJob(job.getKey())
                .withSchedule(CronScheduleBuilder.cronSchedule("0 * * * * ?")).build();
        t.computeFirstFireTime(null);
        return t;
    }

    private OperableTrigger oneShot(String name, JobDetail job) {
        SimpleTriggerImpl t = (SimpleTriggerImpl) TriggerBuilder.newTrigger().withIdentity(name, "g").forJob(job.getKey())
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withRepeatCount(0)).startAt(new Date(System.currentTimeMillis() + 30_000)).build();
        t.computeFirstFireTime(null);
        return t;
    }

    private JobDetail job(String name, Class<? extends org.quartz.Job> c) {
        return JobBuilder.newJob(c).withIdentity("job-" + name, "g").build();
    }

    private List<OperableTrigger> acquireAll() throws Exception {
        return jobStore.acquireNextTriggers(System.currentTimeMillis() + 120_000, 10, 0);
    }

    private String stateOf(OperableTrigger t) {
        String hash = schema.triggerHashKey(t.getKey());
        for (RedisTriggerState s : RedisTriggerState.values()) {
            if (jedis.zscore(schema.triggerStateKey(s), hash) != null) return s.name();
        }
        return "NONE";
    }

    private Double scoreOf(OperableTrigger t, String state) {
        return jedis.zscore(schema.triggerStateKey(RedisTriggerState.valueOf(state)), schema.triggerHashKey(t.getKey()));
    }

    private String lockOf(OperableTrigger t) {
        return jedis.get(schema.triggerLockKey(t.getKey()));
    }

    private String hashField(OperableTrigger t, String field) {
        return jedis.hget(schema.triggerHashKey(t.getKey()), field);
    }

    private boolean jobBlocked(JobDetail j) {
        return jedis.sismember(schema.blockedJobsSet(), schema.jobHashKey(j.getKey()));
    }

    private OperableTrigger find(List<OperableTrigger> l, String name) {
        for (OperableTrigger t : l) if (t.getKey().getName().equals(name)) return t;
        throw new AssertionError("not acquired: " + name);
    }

    // ---- concurrent job ---------------------------------------------------------------------------------------------

    @Test
    public void fireConcurrentJob() throws Exception {
        JobDetail job = job("c", TestJob.class);
        OperableTrigger t = cron("t", job);
        storeJobAndTriggers(job, t);
        List<OperableTrigger> acquired = acquireAll();
        assertEquals("ACQUIRED", stateOf(t));
        assertEquals(SCHEDULER, lockOf(t));

        long before = System.currentTimeMillis();
        List<TriggerFiredResult> results = jobStore.triggersFired(acquired);

        assertEquals(1, results.size());
        assertNotNull(results.get(0).getTriggerFiredBundle());
        assertEquals(job.getKey(), results.get(0).getTriggerFiredBundle().getJobDetail().getKey());
        long next = results.get(0).getTriggerFiredBundle().getNextFireTime().getTime();
        assertEquals("WAITING", stateOf(t));
        assertEquals((double) next, scoreOf(t, "WAITING"), 0.0);
        assertNull("trigger lock stays", lockOf(t));
        assertEquals(Long.toString(next), hashField(t, "nextFireTime"));
        long prev = Long.parseLong(hashField(t, "previousFireTime"));
        assertTrue(prev >= before - 1000 && prev <= System.currentTimeMillis() + 1000);
        assertTrue("concurrent job must not be blocked", !jobBlocked(job));
    }

    @Test
    public void fireConcurrentJobWithoutNextFireTime() throws Exception {
        JobDetail job = job("c1", TestJob.class);
        OperableTrigger t = oneShot("t", job);
        storeJobAndTriggers(job, t);
        List<OperableTrigger> acquired = jobStore.acquireNextTriggers(System.currentTimeMillis() + 120_000, 10, 0);
        jobStore.triggersFired(acquired);

        assertEquals("NONE", stateOf(t));
        assertEquals("", hashField(t, "nextFireTime"));
        assertNotNull(hashField(t, "previousFireTime"));
    }

    @Test
    public void completeConcurrentJobChangesNothing() throws Exception {
        JobDetail job = job("c2", TestJob.class);
        OperableTrigger t = cron("t", job);
        storeJobAndTriggers(job, t);
        List<OperableTrigger> acquired = acquireAll();
        jobStore.triggersFired(acquired);
        Double score = scoreOf(t, "WAITING");
        String next = hashField(t, "nextFireTime");

        jobStore.triggeredJobComplete(acquired.get(0), job, Trigger.CompletedExecutionInstruction.NOOP);

        assertEquals("WAITING", stateOf(t));
        assertEquals(score, scoreOf(t, "WAITING"));
        assertEquals(next, hashField(t, "nextFireTime"));
    }

    // ---- stateful job (no concurrent execution) ----------------------------------------------------------------------

    @Test
    public void fireStatefulJob() throws Exception {
        JobDetail job = job("s", TestJobNonConcurrent.class);
        OperableTrigger t = cron("t", job);
        storeJobAndTriggers(job, t);
        List<OperableTrigger> acquired = acquireAll();

        List<TriggerFiredResult> results = jobStore.triggersFired(acquired);

        long next = results.get(0).getTriggerFiredBundle().getNextFireTime().getTime();
        assertEquals("BLOCKED", stateOf(t));
        assertEquals((double) next, scoreOf(t, "BLOCKED"), 0.0);
        assertEquals(SCHEDULER, lockOf(t));
        assertTrue(jobBlocked(job));
        assertEquals(SCHEDULER, jedis.get(schema.jobBlockedKey(job.getKey())));
        assertEquals(Long.toString(next), hashField(t, "nextFireTime"));
        assertNotNull(hashField(t, "previousFireTime"));
    }

    @Test
    public void fireStatefulJobBlocksWaitingAndPausedSiblings() throws Exception {
        JobDetail job = job("s2", TestJobNonConcurrent.class);
        OperableTrigger fired = cron("fired", job);
        OperableTrigger waiting = cron("waiting", job);
        OperableTrigger paused = cron("paused", job);
        storeJobAndTriggers(job, fired, waiting, paused);
        jobStore.pauseTrigger(paused.getKey());
        assertEquals("PAUSED", stateOf(paused));

        // a job which does not run concurrently gets one trigger acquired per call
        List<OperableTrigger> acquired = acquireAll();
        assertEquals(1, acquired.size());
        OperableTrigger a = acquired.get(0);
        OperableTrigger sibling = a.getKey().getName().equals("fired") ? waiting : fired;
        jobStore.triggersFired(Collections.singletonList(a));

        assertEquals("BLOCKED", stateOf(a));
        assertEquals("BLOCKED", stateOf(sibling));
        assertEquals("PAUSED_BLOCKED", stateOf(paused));
        assertEquals(SCHEDULER, lockOf(a));
        assertEquals(SCHEDULER, lockOf(sibling));
        assertEquals(SCHEDULER, lockOf(paused));
        assertTrue(jobBlocked(job));
    }

    @Test
    public void fireStatefulJobWithoutNextFireTime() throws Exception {
        JobDetail job = job("s3", TestJobNonConcurrent.class);
        OperableTrigger t = oneShot("t", job);
        storeJobAndTriggers(job, t);
        List<OperableTrigger> acquired = jobStore.acquireNextTriggers(System.currentTimeMillis() + 120_000, 10, 0);

        jobStore.triggersFired(acquired);

        assertTrue(jobBlocked(job));
        assertEquals(SCHEDULER, jedis.get(schema.jobBlockedKey(job.getKey())));
        assertNotNull("previous fire time is set", hashField(t, "previousFireTime"));
        assertEquals("ACQUIRED", stateOf(t));
    }

    @Test
    public void completeStatefulJobUnblocksTheTriggers() throws Exception {
        JobDetail job = job("s4", TestJobNonConcurrent.class);
        OperableTrigger fired = cron("fired", job);
        OperableTrigger waiting = cron("waiting", job);
        OperableTrigger paused = cron("paused", job);
        storeJobAndTriggers(job, fired, waiting, paused);
        jobStore.pauseTrigger(paused.getKey());
        List<OperableTrigger> acquired = acquireAll();
        assertEquals(1, acquired.size());
        OperableTrigger a = acquired.get(0);
        OperableTrigger sibling = a.getKey().getName().equals("fired") ? waiting : fired;
        jobStore.triggersFired(Collections.singletonList(a));
        Double aScore = scoreOf(a, "BLOCKED"), siblingScore = scoreOf(sibling, "BLOCKED"), pausedScore = scoreOf(paused, "PAUSED_BLOCKED");

        jobStore.triggeredJobComplete(a, job, Trigger.CompletedExecutionInstruction.NOOP);

        assertEquals("WAITING", stateOf(a));
        assertEquals("WAITING", stateOf(sibling));
        assertEquals("PAUSED", stateOf(paused));
        assertEquals(aScore, scoreOf(a, "WAITING"));
        assertEquals(siblingScore, scoreOf(sibling, "WAITING"));
        assertEquals(pausedScore, scoreOf(paused, "PAUSED"));
        assertTrue("job still blocked", !jobBlocked(job));
        assertNull(jedis.get(schema.jobBlockedKey(job.getKey())));
        assertNull(lockOf(a));
        assertNull(lockOf(sibling));
        assertNull(lockOf(paused));
    }

    @Test
    public void aFullCycleLeavesTheStateOfBefore() throws Exception {
        // acquire, fire, complete, again: nothing piles up
        JobDetail job = job("s5", TestJobNonConcurrent.class);
        OperableTrigger t = cron("t", job);
        storeJobAndTriggers(job, t);
        for (int i = 0; i < 3; i++) {
            jedis.zadd(schema.triggerStateKey(RedisTriggerState.WAITING), System.currentTimeMillis() - 1000, schema.triggerHashKey(t.getKey()));
            List<OperableTrigger> acquired = jobStore.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0);
            assertEquals(1, acquired.size());
            jobStore.triggersFired(acquired);
            assertEquals("BLOCKED", stateOf(t));
            jobStore.triggeredJobComplete(acquired.get(0), job, Trigger.CompletedExecutionInstruction.NOOP);
            assertEquals("WAITING", stateOf(t));
            assertTrue(!jobBlocked(job));
            assertNull(lockOf(t));
            assertEquals(0L, (long) jedis.zcard(schema.triggerStateKey(RedisTriggerState.ACQUIRED)));
            assertEquals(0L, (long) jedis.zcard(schema.triggerStateKey(RedisTriggerState.BLOCKED)));
        }
    }
}
