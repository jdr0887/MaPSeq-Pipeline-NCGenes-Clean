package edu.unc.mapseq.pipeline.ncgenes.clean;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCGenesCleanPipelineExecutorService {

    private final Logger logger = LoggerFactory.getLogger(NCGenesCleanPipelineExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCGenesCleanPipelineExecutorTask task;

    private Long period = 5L;

    public NCGenesCleanPipelineExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000; // 1 minute
        mainTimer.scheduleAtFixedRate(task, delay, period);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCGenesCleanPipelineExecutorTask getTask() {
        return task;
    }

    public void setTask(NCGenesCleanPipelineExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
