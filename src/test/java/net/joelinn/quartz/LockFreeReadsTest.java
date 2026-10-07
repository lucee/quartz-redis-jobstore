package net.joelinn.quartz;

import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import redis.clients.jedis.params.SetParams;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Read-only operations do not take the global Redis lock; write operations still do.
 */
public class LockFreeReadsTest extends BaseTest {

    @Test
    public void readsReturnStoredData() throws Exception {
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = getJobsAndTriggers(2, 5, 1, 2);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        assertEquals(10, jobStore.getNumberOfJobs());
        assertEquals(20, jobStore.getNumberOfTriggers());
        assertEquals(2, jobStore.getJobGroupNames().size());

        Set<TriggerKey> keys = jobStore.getTriggerKeys(GroupMatcher.<TriggerKey>anyGroup());
        assertEquals(20, keys.size());
        for (TriggerKey key : keys) {
            OperableTrigger trigger = jobStore.retrieveTrigger(key);
            assertNotNull(trigger);
            assertEquals(key, trigger.getKey());
            assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(key));
            assertTrue(jobStore.checkExists(key));
            assertNotNull(jobStore.retrieveJob(trigger.getJobKey()));
        }
        Set<JobKey> jobKeys = jobStore.getJobKeys(GroupMatcher.<JobKey>anyGroup());
        assertEquals(10, jobKeys.size());
        for (JobKey jobKey : jobKeys) {
            assertEquals(2, jobStore.getTriggersForJob(jobKey).size());
        }
    }

    @Test
    public void readsDoNotWaitForGlobalLock() throws Exception {
        JobDetail job = getJobDetail();
        OperableTrigger trigger = getCronTrigger("testTrigger", "testTriggerGroup", job.getKey());
        storeJobAndTriggers(job, trigger);
        TriggerKey triggerKey = trigger.getKey();

        // another client holds the global lock (no expiry)
        jedis.set(schema.lockKey(), "held-by-someone-else");
        try {
            long start = System.nanoTime();
            assertNotNull(jobStore.retrieveTrigger(triggerKey));
            assertNotNull(jobStore.retrieveJob(job.getKey()));
            assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(triggerKey));
            assertEquals(1, jobStore.getNumberOfTriggers());
            long millis = (System.nanoTime() - start) / 1_000_000;
            assertTrue("reads waited for the lock: " + millis + " ms", millis < 1000);
        } finally {
            jedis.del(schema.lockKey());
        }
    }

    @Test
    public void writesStillWaitForGlobalLock() throws Exception {
        // lock held by someone else, expires after 1.5 s
        jedis.set(schema.lockKey(), "held-by-someone-else", SetParams.setParams().px(1500));
        long start = System.nanoTime();
        jobStore.storeJob(getJobDetail(), false);
        long millis = (System.nanoTime() - start) / 1_000_000;
        assertTrue("write did not wait for the lock: " + millis + " ms", millis >= 1000);
        assertEquals(1, jobStore.getNumberOfJobs());
    }
}
