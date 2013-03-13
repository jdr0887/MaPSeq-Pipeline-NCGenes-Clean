package edu.unc.mapseq.pipeline.ncgenes.clean;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCGenesCleanPipelineExecutorService {

    private final Logger logger = LoggerFactory.getLogger(NCGenesCleanPipelineExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCGenesCleanPipelineBeanService pipelineBeanService;

    public void start() throws Exception {
        logger.info("ENTERING stop()");

        long delay = 15 * 1000; // 15 seconds
        long period = 5 * 60 * 1000; // 5 minutes

        NCGenesCleanPipelineExecutorTask task = new NCGenesCleanPipelineExecutorTask();
        task.setPipelineBeanService(pipelineBeanService);
        mainTimer.scheduleAtFixedRate(task, delay, period);

    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCGenesCleanPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(NCGenesCleanPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
