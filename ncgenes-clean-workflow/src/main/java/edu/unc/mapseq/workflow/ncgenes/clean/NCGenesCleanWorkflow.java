package edu.unc.mapseq.workflow.ncgenes.clean;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowUtil;
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class NCGenesCleanWorkflow extends AbstractSampleWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NCGenesCleanWorkflow.class);

    public NCGenesCleanWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesCleanWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/ncgenes/clean/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            logger.debug(sample.toString());

            Flowcell flowcell = sample.getFlowcell();
            File outputDirectory = new File(sample.getOutputDirectory(), getName().replace("Clean", ""));
            File tmpDirectory = new File(outputDirectory, "tmp");
            tmpDirectory.mkdirs();

            List<File> readPairList = WorkflowUtil.getReadPairList(sample.getFileDatas(), flowcell.getName(),
                    sample.getLaneIndex());

            if (readPairList.size() == 2) {

                File r1FastqFile = readPairList.get(0);
                String r1FastqRootName = WorkflowUtil.getRootFastqName(r1FastqFile.getName());

                File r2FastqFile = readPairList.get(1);
                String r2FastqRootName = WorkflowUtil.getRootFastqName(r2FastqFile.getName());

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

                    CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt)
                            .siteName(siteName);
                    for (File f : deleteFileList) {
                        builder.addArgument(RemoveCLI.FILE, f.getAbsolutePath());
                    }
                    CondorJob removeJob = builder.build();
                    logger.info(removeJob.toString());
                    graph.addVertex(removeJob);

                } catch (Exception e) {
                    throw new WorkflowException(e);
                }

            }

        }

        return graph;
    }

}
