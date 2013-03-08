package edu.unc.mapseq.pipeline.ncgenes.clean;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.config.RunModeType;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.pipeline.AbstractPipeline;
import edu.unc.mapseq.pipeline.PipelineException;
import edu.unc.mapseq.pipeline.PipelineUtil;

public class NCGenesCleanPipeline extends AbstractPipeline<NCGenesCleanPipelineBeanService> {

    private final Logger logger = LoggerFactory.getLogger(NCGenesCleanPipeline.class);

    private NCGenesCleanPipelineBeanService pipelineBeanService;

    public NCGenesCleanPipeline() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesCleanPipeline.class.getSimpleName().replace("Pipeline", "").toUpperCase();
    }

    @Override
    public String getVersion() {
        Properties props = new Properties();
        try {
            props.load(this.getClass().getResourceAsStream("pipeline.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props.getProperty("version", "0.0.1-SNAPSHOT");
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws PipelineException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;
        String version = null;

        if (getWorkflowPlan().getSequencerRun() == null && getWorkflowPlan().getHTSFSamples() == null) {
            logger.error("Don't have either sequencerRun and htsfSample");
            throw new PipelineException("Don't have either sequencerRun and htsfSample");
        }

        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();

        if (getWorkflowPlan().getSequencerRun() != null) {
            logger.info("sequencerRun: {}", getWorkflowPlan().getSequencerRun().toString());
            try {
                htsfSampleSet.addAll(this.pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO()
                        .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId()));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (getWorkflowPlan().getHTSFSamples() != null) {
            logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        RunModeType runMode = getPipelineBeanService().getMapseqConfigurationService().getRunMode();

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample.getName(), getName()
                    .replace("Clean", ""));
            File tmpDir = new File(outputDirectory, "tmp");
            tmpDir.mkdirs();

            Set<EntityAttribute> attributeSet = htsfSample.getAttributes();
            Iterator<EntityAttribute> attributeIter = attributeSet.iterator();
            while (attributeIter.hasNext()) {
                EntityAttribute attribute = attributeIter.next();
                if ("GATKDepthOfCoverage.interval_list.version".equals(attribute.getName())) {
                    version = attribute.getValue();
                }
            }

            if (version == null) {
                throw new PipelineException("Version is null...returning empty dag");
            }

            File intervalListByVersionFile = new File(String.format(
                    "/proj/renci/sequence_analysis/annotation/abeast/NCGenes/%1$s/exons_pm_0_v%1$s.interval_list",
                    version));
            if (!intervalListByVersionFile.exists()) {
                throw new PipelineException("Interval list file does not exist: "
                        + intervalListByVersionFile.getAbsolutePath());
            }

            logger.debug("htsfSample = {}", htsfSample.toString());
            List<File> readPairList = PipelineUtil.getReadPairList(htsfSample.getFileDatas(), sequencerRun.getName(),
                    htsfSample.getLaneIndex());
            logger.debug("fileList = {}", readPairList.size());

            // assumption: a dash is used as a delimiter between a participantId
            // and the external code
            int idx = htsfSample.getName().lastIndexOf("-");
            String participantId = idx != -1 ? htsfSample.getName().substring(0, idx) : htsfSample.getName();

            if (readPairList.size() == 2) {

                File r1FastqFile = readPairList.get(0);
                String r1FastqRootName = PipelineUtil.getRootFastqName(r1FastqFile.getName());

                File r2FastqFile = readPairList.get(1);
                String r2FastqRootName = PipelineUtil.getRootFastqName(r2FastqFile.getName());

                String fastqLaneRootName = StringUtils.removeEnd(r2FastqRootName, "_R2");

                try {

                } catch (Exception e) {
                    throw new PipelineException(e);
                }

            }

        }

        return graph;
    }

    public NCGenesCleanPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(NCGenesCleanPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
