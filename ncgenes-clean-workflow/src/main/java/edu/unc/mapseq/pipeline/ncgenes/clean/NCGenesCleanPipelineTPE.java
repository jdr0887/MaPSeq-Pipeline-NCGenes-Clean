package edu.unc.mapseq.pipeline.ncgenes.clean;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NCGenesCleanPipelineTPE extends ThreadPoolExecutor {

    public NCGenesCleanPipelineTPE() {
        super(20, 20, 5L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

}
