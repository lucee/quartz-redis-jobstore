package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.RedisJobStore;
import net.joelinn.quartz.jobstore.TriggerDetails;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BulkTriggerDetailsTest extends BaseTest {

    private Map<TriggerKey, TriggerDetails> byKey(java.util.List<TriggerDetails> all) {
        Map<TriggerKey, TriggerDetails> map = new HashMap<>();
        for (TriggerDetails d : all) {
            map.put(d.getTrigger().getKey(), d);
        }
        assertEquals("duplicate triggers returned", all.size(), map.size());
        return map;
    }

    /** the bulk result must be identical to what the per-key operations return */
    private void assertSameAsPerKey(int expectedTriggers) throws Exception {
        Map<TriggerKey, TriggerDetails> bulk = byKey(jobStore.getAllTriggerDetails());
        Set<TriggerKey> keys = jobStore.getTriggerKeys(GroupMatcher.<TriggerKey>anyGroup());
        assertEquals(expectedTriggers, keys.size());
        assertEquals(keys, bulk.keySet());
        for (TriggerKey key : keys) {
            TriggerDetails d = bulk.get(key);
            OperableTrigger expected = jobStore.retrieveTrigger(key);
            assertEquals(expected.getKey(), d.getTrigger().getKey());
            assertEquals(expected.getJobKey(), d.getTrigger().getJobKey());
            assertEquals(expected.getDescription(), d.getTrigger().getDescription());
            assertEquals(expected.getStartTime(), d.getTrigger().getStartTime());
            assertEquals(expected.getNextFireTime(), d.getTrigger().getNextFireTime());
            assertEquals(expected.getPreviousFireTime(), d.getTrigger().getPreviousFireTime());
            assertEquals(expected.getFireInstanceId(), d.getTrigger().getFireInstanceId());
            assertEquals(((CronTriggerImpl) expected).getCronExpression(), ((CronTriggerImpl) d.getTrigger()).getCronExpression());
            assertEquals(expected.getJobDataMap(), d.getTrigger().getJobDataMap());
            assertEquals(jobStore.getTriggerState(key), d.getState());

            JobDetail expectedJob = jobStore.retrieveJob(expected.getJobKey());
            assertNotNull(d.getJobDetail());
            assertEquals(expectedJob.getKey(), d.getJobDetail().getKey());
            assertEquals(expectedJob.getDescription(), d.getJobDetail().getDescription());
            assertEquals(expectedJob.getJobClass(), d.getJobDetail().getJobClass());
            assertEquals(expectedJob.getJobDataMap(), d.getJobDetail().getJobDataMap());
        }
    }

    @Test
    public void emptyStore() throws Exception {
        assertTrue(jobStore.getAllTriggerDetails().isEmpty());
    }

    @Test
    public void sameAsPerKeyReads() throws Exception {
        // 4 jobs, 2 triggers each, in 2 trigger groups
        jobStore.storeJobsAndTriggers(getJobsAndTriggers(2, 2, 1, 2), false);
        assertSameAsPerKey(8);
    }

    @Test
    public void statesAreReported() throws Exception {
        jobStore.storeJobsAndTriggers(getJobsAndTriggers(1, 4, 1, 1), false);
        Set<TriggerKey> keys = jobStore.getTriggerKeys(GroupMatcher.<TriggerKey>anyGroup());
        TriggerKey paused = keys.iterator().next();
        jobStore.pauseTrigger(paused);

        Map<TriggerKey, TriggerDetails> bulk = byKey(jobStore.getAllTriggerDetails());
        assertEquals(Trigger.TriggerState.PAUSED, bulk.get(paused).getState());
        for (TriggerKey key : keys) {
            if (!key.equals(paused)) {
                assertEquals(Trigger.TriggerState.NORMAL, bulk.get(key).getState());
            }
        }
        assertSameAsPerKey(4);
    }

    @Test
    public void pausedGroup() throws Exception {
        jobStore.storeJobsAndTriggers(getJobsAndTriggers(1, 3, 1, 1), false);
        String group = jobStore.getTriggerGroupNames().get(0);
        jobStore.pauseTriggers(GroupMatcher.<TriggerKey>triggerGroupEquals(group));
        assertSameAsPerKey(3);
        for (TriggerDetails d : jobStore.getAllTriggerDetails()) {
            if (d.getTrigger().getKey().getGroup().equals(group)) {
                assertEquals(Trigger.TriggerState.PAUSED, d.getState());
            }
        }
    }

    @Test
    public void severalTriggersShareOneJob() throws Exception {
        JobDetail job = getJobDetail();
        CronTriggerImpl t1 = getCronTrigger("t1", "g", job.getKey());
        CronTriggerImpl t2 = getCronTrigger("t2", "g", job.getKey());
        storeJobAndTriggers(job, t1, t2);
        Map<TriggerKey, TriggerDetails> bulk = byKey(jobStore.getAllTriggerDetails());
        assertEquals(2, bulk.size());
        assertNotNull(bulk.get(t1.getKey()).getJobDetail());
        assertEquals(job.getKey(), bulk.get(t2.getKey()).getJobDetail().getKey());
        assertSameAsPerKey(2);
    }

    @Test
    public void triggerWithoutJobHasNullJob() throws Exception {
        JobDetail job = getJobDetail();
        CronTriggerImpl trigger = getCronTrigger("t1", "g", job.getKey());
        storeJobAndTriggers(job, trigger);
        // orphan the trigger by removing only the job hash
        jedis.del(schema.jobHashKey(job.getKey()));
        TriggerDetails d = byKey(jobStore.getAllTriggerDetails()).get(trigger.getKey());
        assertNotNull(d);
        assertNull(d.getJobDetail());
    }

    @Test
    public void moreThanOneChunk() throws Exception {
        // chunk size is 500: use more triggers (and jobs) than that
        jobStore.storeJobsAndTriggers(getJobsAndTriggers(1, 620, 1, 1), false);
        assertEquals(620, jobStore.getAllTriggerDetails().size());
        assertSameAsPerKey(620);
    }
}
