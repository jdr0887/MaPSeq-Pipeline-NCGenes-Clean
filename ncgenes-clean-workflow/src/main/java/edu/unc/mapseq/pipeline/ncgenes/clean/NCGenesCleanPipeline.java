package edu.unc.mapseq.pipeline.ncgenes.clean;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.pipeline.AbstractPipeline;
import edu.unc.mapseq.pipeline.PipelineException;
import edu.unc.mapseq.pipeline.PipelineJobFactory;
import edu.unc.mapseq.pipeline.PipelineUtil;

public class NCGenesCleanPipeline extends AbstractPipeline {

    private final Logger logger = LoggerFactory.getLogger(NCGenesCleanPipeline.class);

    public NCGenesCleanPipeline() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesCleanPipeline.class.getSimpleName().replace("Pipeline", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/pipeline/ncgenes/clean/pipeline");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws PipelineException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        if (getWorkflowPlan().getSequencerRun() == null && getWorkflowPlan().getHTSFSamples() == null) {
            logger.error("Don't have either sequencerRun and htsfSample");
            throw new PipelineException("Don't have either sequencerRun and htsfSample");
        }

        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();

        if (getWorkflowPlan().getSequencerRun() != null) {
            logger.info("sequencerRun: {}", getWorkflowPlan().getSequencerRun().toString());
            try {
                htsfSampleSet.addAll(getPipelineBeanService().getMaPSeqDAOBean().getHTSFSampleDAO()
                        .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId()));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (getWorkflowPlan().getHTSFSamples() != null) {
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        String siteName = getPipelineBeanService().getAttributes().get("siteName");

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample,
                    getName().replace("Clean", ""), getVersion());

            logger.debug("htsfSample = {}", htsfSample.toString());
            List<File> readPairList = PipelineUtil.getReadPairList(htsfSample.getFileDatas(), sequencerRun.getName(),
                    htsfSample.getLaneIndex());

            if (readPairList.size() == 2) {

                File r1FastqFile = readPairList.get(0);
                String r1FastqRootName = PipelineUtil.getRootFastqName(r1FastqFile.getName());

                File r2FastqFile = readPairList.get(1);
                String r2FastqRootName = PipelineUtil.getRootFastqName(r2FastqFile.getName());

                String fastqLaneRootName = StringUtils.removeEnd(r2FastqRootName, "_R2");

                try {

                    List<File> deleteFileList = new ArrayList<File>();

                    File fastqcR1Output = new File(outputDirectory, r1FastqRootName + ".fastqc.zip");

                    File saiR1OutFile = new File(outputDirectory, r1FastqRootName + ".sai");
                    deleteFileList.add(saiR1OutFile);

                    File fastqcR2Output = new File(outputDirectory, r2FastqRootName + ".fastqc.zip");

                    File saiR2OutFile = new File(outputDirectory, r2FastqRootName + ".sai");
                    deleteFileList.add(saiR2OutFile);

                    File bwaSAMPairedEndOutFile = new File(outputDirectory, fastqLaneRootName + ".sam");
                    deleteFileList.add(bwaSAMPairedEndOutFile);

                    File fixRGOutput = new File(outputDirectory, bwaSAMPairedEndOutFile.getName().replace(".sam",
                            ".fixed-rg.bam"));
                    deleteFileList.add(fixRGOutput);

                    File picardAddOrReplaceReadGroupsIndexOut = new File(outputDirectory, fixRGOutput.getName()
                            .replace(".bam", ".bai"));
                    deleteFileList.add(picardAddOrReplaceReadGroupsIndexOut);

                    File picardMarkDuplicatesMetricsFile = new File(outputDirectory, fixRGOutput.getName().replace(
                            ".bam", ".deduped.metrics"));

                    File picardMarkDuplicatesOutput = new File(outputDirectory, fixRGOutput.getName().replace(".bam",
                            ".deduped.bam"));

                    File picardMarkDuplicatesIndexOut = new File(outputDirectory, picardMarkDuplicatesOutput.getName()
                            .replace(".bam", ".bai"));

                    File realignTargetCreatorOut = new File(outputDirectory, picardMarkDuplicatesOutput.getName()
                            .replace(".bam", ".targets.intervals"));
                    deleteFileList.add(realignTargetCreatorOut);

                    File indelRealignerOut = new File(outputDirectory, picardMarkDuplicatesOutput.getName().replace(
                            ".bam", ".realign.bam"));
                    deleteFileList.add(indelRealignerOut);

                    File picardFixMateOutput = new File(outputDirectory, indelRealignerOut.getName().replace(".bam",
                            ".fixmate.bam"));
                    deleteFileList.add(picardFixMateOutput);

                    File picardFixMateIndexOut = new File(outputDirectory, picardFixMateOutput.getName().replace(
                            ".bam", ".bai"));
                    deleteFileList.add(picardFixMateIndexOut);

                    File gatkCountCovariatesRecalFile = new File(outputDirectory, picardFixMateOutput.getName()
                            .replace(".bam", ".bam.cov"));

                    File gatkTableRecalibrationOut = new File(outputDirectory, picardFixMateOutput.getName().replace(
                            ".bam", ".recal.bam"));

                    File gatkTableRecalibrationIndexOut = new File(outputDirectory, gatkTableRecalibrationOut.getName()
                            .replace(".bam", ".bai"));

                    File samtoolsFlagstatOut = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(
                            ".bam", ".samtools.flagstat"));

                    File gatkFlagstatOut = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(
                            ".bam", ".gatk.flagstat"));

                    // gatkDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.OUTPUTPREFIX, gatkTableRecalibrationOut
                    // .getName().replace(".bam", ".coverage"));

                    File gatkUnifiedGenotyperOut = new File(outputDirectory, gatkTableRecalibrationOut.getName()
                            .replace(".bam", ".vcf"));

                    File gatkUnifiedGenotyperMetrics = new File(outputDirectory, gatkTableRecalibrationOut.getName()
                            .replace(".bam", ".metrics"));

                    File filterVariant1Output = new File(outputDirectory, gatkTableRecalibrationOut.getName().replace(
                            ".bam", ".variant.vcf"));

                    File gatkVariantRecalibratorRecalFile = new File(outputDirectory, filterVariant1Output.getName()
                            .replace(".vcf", ".recal"));

                    File gatkVariantRecalibratorTranchesFile = new File(outputDirectory, filterVariant1Output.getName()
                            .replace(".vcf", ".tranches"));

                    File gatkVariantRecalibratorRScriptFile = new File(outputDirectory, filterVariant1Output.getName()
                            .replace(".vcf", ".plots.R"));

                    File gatkApplyRecalibrationOut = new File(outputDirectory, filterVariant1Output.getName().replace(
                            ".vcf", ".recalibrated.filtered.vcf"));

                    File filterVariant2Output = new File(outputDirectory, filterVariant1Output.getName().replace(
                            ".vcf", ".ic_snps.vcf"));

                    CondorJob removeJob = PipelineJobFactory.createJob(++count, RemoveCLI.class, getWorkflowPlan(),
                            htsfSample);
                    removeJob.setSiteName(siteName);
                    for (File f : deleteFileList) {
                        removeJob.addArgument(RemoveCLI.FILE, f.getAbsolutePath());
                    }
                    graph.addVertex(removeJob);

                } catch (Exception e) {
                    throw new PipelineException(e);
                }

            }

        }

        return graph;
    }

}
