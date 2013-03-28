package edu.unc.mapseq.pipeline.ncgenes.clean;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowPlanDAO;
import edu.unc.mapseq.dao.WorkflowRunDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowPlan;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.pipeline.PipelineExecutor;

public class NCGenesCleanPipelineExecutorTask extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(NCGenesCleanPipelineExecutorTask.class);

    private static final NCGenesCleanPipelineTPE tpe = new NCGenesCleanPipelineTPE();

    private NCGenesCleanPipelineBeanService pipelineBeanService;

    public NCGenesCleanPipelineExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d", tpe.getActiveCount(),
                tpe.getTaskCount(), tpe.getCompletedTaskCount()));

        WorkflowDAO workflowDAO = this.pipelineBeanService.getMaPSeqDAOBean().getWorkflowDAO();
        WorkflowRunDAO workflowRunDAO = this.pipelineBeanService.getMaPSeqDAOBean().getWorkflowRunDAO();
        WorkflowPlanDAO workflowPlanDAO = this.pipelineBeanService.getMaPSeqDAOBean().getWorkflowPlanDAO();

        try {
            Workflow workflow = workflowDAO.findByName("NCGenesClean");
            List<WorkflowPlan> workflowPlanList = workflowPlanDAO.findEnqueued(workflow.getId());

            if (workflowPlanList != null && workflowPlanList.size() > 0) {

                logger.info("dequeuing {} WorkflowPlans", workflowPlanList.size());
                for (WorkflowPlan workflowPlan : workflowPlanList) {

                    NCGenesCleanPipeline pipeline = new NCGenesCleanPipeline();

                    WorkflowRun workflowRun = workflowPlan.getWorkflowRun();
                    workflowRun.setVersion(pipeline.getVersion());
                    workflowRun.setDequeuedDate(new Date());
                    workflowRunDAO.save(workflowRun);

                    pipeline.setPipelineBeanService(pipelineBeanService);
                    pipeline.setWorkflowPlan(workflowPlan);
                    tpe.submit(new PipelineExecutor(pipeline));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public NCGenesCleanPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(NCGenesCleanPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
