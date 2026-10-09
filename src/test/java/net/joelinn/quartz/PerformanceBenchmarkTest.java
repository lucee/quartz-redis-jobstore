package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.RedisJobStore;
import net.joelinn.quartz.jobstore.RedisTriggerState;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * Manual benchmark, skipped unless -Dbenchmark=true is set. Times the main operations of the job store and expresses them in
 * round trips to Redis (the cost of a PING), which is what matters when Redis is not on the same machine.
 *
 * Add network latency to see the real effect, for example a Redis in docker with
 *   tc qdisc add dev eth0 root netem delay 5ms
 * and run it with
 *   REDIS_SERVER=127.0.0.1 REDIS_PORT=6390 mvn test -Dtest=PerformanceBenchmarkTest -Dbenchmark=true
 * Options: -Dperf.heartbeat=0 (no heartbeat thread), -Dperf.testOnBorrow=0 (no PING when a connection is taken from the pool).
 */
public class PerformanceBenchmarkTest extends BaseTest {
    interface Op { void run() throws Exception; }

    private double pingMs;
    private final List<String> rows = new ArrayList<>();

    private void time(String name, int n, Op prep, Op op) throws Exception {
        long total = 0; int counted = 0;
        for (int i = 0; i < n + 3; i++) {
            if (prep != null) prep.run();
            long t = System.nanoTime();
            op.run();
            long d = System.nanoTime() - t;
            if (i >= 3) { total += d; counted++; }
        }
        double ms = total / 1e6 / counted;
        rows.add(String.format("PERF %-34s %8.1f ms  = %5.1f round trips", name, ms, ms / pingMs));
    }

    private OperableTrigger trig(String name, JobKey job) {
        CronTriggerImpl t = (CronTriggerImpl) TriggerBuilder.newTrigger().withIdentity("trigger-" + name, "g").forJob(job)
                .withSchedule(CronScheduleBuilder.cronSchedule("0 * * * * ?")).build();
        t.computeFirstFireTime(null);
        return t;
    }

    @Test
    public void perf() throws Exception {
        org.junit.Assume.assumeTrue("manual benchmark, run with -Dbenchmark=true", Boolean.getBoolean("benchmark"));
        RedisJobStore s = new RedisJobStore();
        s.setHost(host); s.setPort(port); s.setDatabase(1); s.setLockTimeout(5000); s.setInstanceId("perf");
        boolean hb = !"0".equals(System.getProperty("perf.heartbeat", "1"));
        s.setHeartbeatInterval(hb ? 60_000 : 0);
        s.setTestOnBorrow(!"0".equals(System.getProperty("perf.testOnBorrow", "1")));
        s.initialize(null, mockScheduleSignaler);
        if (hb) s.schedulerStarted();
        rows.add("PERF config: heartbeat=" + hb + " testOnBorrow=" + System.getProperty("perf.testOnBorrow", "1"));
        try (Jedis side = new Jedis(host, port)) {
            side.select(1);
            // ping = one round trip
            long pt = 0; for (int i = 0; i < 30; i++) { long t = System.nanoTime(); side.ping(); if (i >= 5) pt += System.nanoTime() - t; }
            pingMs = pt / 1e6 / 25;
            rows.add(String.format("PERF one round trip (PING) = %.2f ms", pingMs));

            // 200 idle triggers in the store, like a real schedule
            Map<JobDetail, Set<? extends Trigger>> bulk = new HashMap<>();
            for (int i = 0; i < 200; i++) {
                JobDetail j = JobBuilder.newJob(TestJob.class).withIdentity("idle-" + i, "g").build();
                CronTriggerImpl t = (CronTriggerImpl) TriggerBuilder.newTrigger().withIdentity("idle-" + i, "g").forJob(j.getKey())
                        .withSchedule(CronScheduleBuilder.cronSchedule("0 0 0 1 1 ? 2099")).build();
                t.computeFirstFireTime(null);
                bulk.put(j, Collections.singleton(t));
            }
            s.storeJobsAndTriggers(bulk, false);

            JobDetail cj = JobBuilder.newJob(TestJob.class).withIdentity("job-c", "g").build();
            JobDetail sj = JobBuilder.newJob(TestJobNonConcurrent.class).withIdentity("job-s", "g").build();
            OperableTrigger ct = trig("c", cj.getKey()), st = trig("s", sj.getKey());
            s.storeJobAndTrigger(cj, ct);
            s.storeJobAndTrigger(sj, st);

            final String ch = schema.triggerHashKey(ct.getKey()), sh = schema.triggerHashKey(st.getKey());
            Op makeDueC = () -> due(side, ch), makeDueS = () -> due(side, sh);
            long now = System.currentTimeMillis();

            time("lock + SCARD (getNumberOfJobs)", 20, null, () -> s.getNumberOfJobs());
            time("acquire, nothing due (idle poll)", 20, null, () -> s.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0));
            time("acquire 1 trigger, concurrent job", 20, makeDueC, () -> s.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0));
            time("acquire 1 trigger, stateful job", 20, makeDueS, () -> s.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0));
            final List<OperableTrigger>[] got = new List[1];
            Op acqC = () -> { makeDueC.run(); got[0] = s.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0); };
            Op acqS = () -> { makeDueS.run(); got[0] = s.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0); };
            time("fire, concurrent job", 20, acqC, () -> s.triggersFired(got[0]));
            time("fire, stateful job", 20, acqS, () -> s.triggersFired(got[0]));
            time("complete, concurrent job", 20, () -> { acqC.run(); s.triggersFired(got[0]); },
                    () -> s.triggeredJobComplete(got[0].get(0), cj, Trigger.CompletedExecutionInstruction.NOOP));
            time("complete, stateful job", 20, () -> { acqS.run(); s.triggersFired(got[0]); },
                    () -> s.triggeredJobComplete(got[0].get(0), sj, Trigger.CompletedExecutionInstruction.NOOP));
            time("FULL CYCLE concurrent (acq+fire+done)", 15, makeDueC, () -> {
                List<OperableTrigger> a = s.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0);
                s.triggersFired(a); s.triggeredJobComplete(a.get(0), cj, Trigger.CompletedExecutionInstruction.NOOP); });
            time("FULL CYCLE stateful (acq+fire+done)", 15, makeDueS, () -> {
                List<OperableTrigger> a = s.acquireNextTriggers(System.currentTimeMillis() + 1000, 1, 0);
                s.triggersFired(a); s.triggeredJobComplete(a.get(0), sj, Trigger.CompletedExecutionInstruction.NOOP); });
            time("retrieveTrigger", 20, null, () -> s.retrieveTrigger(ct.getKey()));
            time("retrieveJob", 20, null, () -> s.retrieveJob(cj.getKey()));
            time("getTriggerState", 20, null, () -> s.getTriggerState(ct.getKey()));
            time("getTriggerKeys (201 triggers)", 10, null, () -> s.getTriggerKeys(GroupMatcher.<TriggerKey>anyGroup()));
            time("pauseTrigger", 20, null, () -> s.pauseTrigger(ct.getKey()));
            time("resumeTrigger", 20, () -> s.pauseTrigger(ct.getKey()), () -> s.resumeTrigger(ct.getKey()));
            final int[] n = {0};
            time("storeJobAndTrigger (new)", 15, null, () -> {
                JobDetail j = JobBuilder.newJob(TestJob.class).withIdentity("tmp-" + (n[0]++), "g").build();
                s.storeJobAndTrigger(j, trig("tmp-" + (n[0] - 1), j.getKey())); });
            final int[] m = {0};
            time("removeJob (with its trigger)", 15, null, () -> s.removeJob(new JobKey("tmp-" + (m[0]++), "g")));
        }
        for (String r : rows) System.out.println(r);
    }

    private static void due(Jedis side, String hash) {
        for (RedisTriggerState st : RedisTriggerState.values()) side.zrem("" + st.getKey(), hash);
        side.zadd(RedisTriggerState.WAITING.getKey(), System.currentTimeMillis() - 1000, hash);
        side.del("trigger_lock:g:" + hash.substring(hash.lastIndexOf(':') + 1));
    }
}
