package net.joelinn.quartz.jobstore;

import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.spi.OperableTrigger;

/**
 * A trigger together with its job and its current state, as returned by
 * {@link RedisJobStore#getAllTriggerDetails()}.
 */
public final class TriggerDetails {
    private final OperableTrigger trigger;
    private final JobDetail jobDetail;
    private final Trigger.TriggerState state;

    public TriggerDetails(OperableTrigger trigger, JobDetail jobDetail, Trigger.TriggerState state) {
        this.trigger = trigger;
        this.jobDetail = jobDetail;
        this.state = state;
    }

    /** @return the trigger (never null) */
    public OperableTrigger getTrigger() {
        return trigger;
    }

    /** @return the job the trigger belongs to; null if the job does not exist (orphaned trigger) */
    public JobDetail getJobDetail() {
        return jobDetail;
    }

    /** @return the state of the trigger */
    public Trigger.TriggerState getState() {
        return state;
    }
}
