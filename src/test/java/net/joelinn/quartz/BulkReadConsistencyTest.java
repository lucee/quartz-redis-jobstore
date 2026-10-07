package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.TriggerDetails;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * getAllTriggerDetails() must return a consistent snapshot while writers are active. A writer moves a trigger
 * between state sets in several steps, and replaces a job together with its data map; the bulk read takes the
 * global lock once and therefore never sees such a half finished update.
 * Worst case load: one trigger is paused and resumed in a tight loop, its job is rewritten in a tight loop.
 */
public class BulkReadConsistencyTest extends BaseTest {

    @Test
    public void bulkReadNeverSeesHalfFinishedUpdates() throws Exception {
        final JobDetail job = JobBuilder.newJob(TestJob.class).withIdentity("sj", "g")
                .usingJobData("label", "x").usingJobData("schedule", "cron").build();
        CronTriggerImpl trigger = getCronTrigger("st", "g", job.getKey());
        storeJobAndTriggers(job, trigger);
        final TriggerKey triggerKey = trigger.getKey();

        final long end = System.currentTimeMillis() + 3000;
        final AtomicLong writes = new AtomicLong(), reads = new AtomicLong(), noState = new AtomicLong(),
                noJob = new AtomicLong(), emptyDataMap = new AtomicLong();
        final Throwable[] failure = new Throwable[1];

        Thread stateWriter = new Thread(() -> {
            try {
                while (System.currentTimeMillis() < end) {
                    jobStore.pauseTrigger(triggerKey);
                    jobStore.resumeTrigger(triggerKey);
                    writes.addAndGet(2);
                }
            } catch (Throwable t) { failure[0] = t; }
        });
        Thread jobWriter = new Thread(() -> {
            try {
                while (System.currentTimeMillis() < end) {
                    jobStore.storeJob(job, true);
                    writes.incrementAndGet();
                }
            } catch (Throwable t) { failure[0] = t; }
        });
        Thread reader = new Thread(() -> {
            try {
                while (System.currentTimeMillis() < end) {
                    for (TriggerDetails d : jobStore.getAllTriggerDetails()) {
                        reads.incrementAndGet();
                        if (d.getState() == Trigger.TriggerState.NONE) noState.incrementAndGet();
                        if (d.getJobDetail() == null) noJob.incrementAndGet();
                        else if (d.getJobDetail().getJobDataMap().isEmpty()) emptyDataMap.incrementAndGet();
                    }
                }
            } catch (Throwable t) { failure[0] = t; }
        });
        Thread[] threads = {stateWriter, jobWriter, reader};
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        if (failure[0] != null) {
            throw new AssertionError("thread failed", failure[0]);
        }
        System.out.println("bulk consistency: writes=" + writes + " reads=" + reads);
        assertTrue("too few operations: writes=" + writes + " reads=" + reads, writes.get() > 50 && reads.get() > 50);
        assertEquals("trigger without state", 0, noState.get());
        assertEquals("trigger without job", 0, noJob.get());
        assertEquals("job with empty data map", 0, emptyDataMap.get());
    }
}
