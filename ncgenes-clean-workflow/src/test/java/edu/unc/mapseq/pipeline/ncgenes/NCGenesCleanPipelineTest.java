package edu.unc.mapseq.pipeline.ncgenes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorDOTExporter;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;

import edu.unc.mapseq.module.bwa.BWAAlignCLI;
import edu.unc.mapseq.module.bwa.BWASAMPairedEndCLI;
import edu.unc.mapseq.module.core.WriteVCFHeaderCLI;
import edu.unc.mapseq.module.fastqc.FastQCCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;

public class NCGenesCleanPipelineTest {

    @Test
    public void createDot() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        // new job
        CondorJob writeVCFHeaderJob = new CondorJob(String.format("%s_%d", WriteVCFHeaderCLI.class.getSimpleName(),
                ++count), null);
        graph.addVertex(writeVCFHeaderJob);

        // new job
        CondorJob fastQCR1Job = new CondorJob(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(fastQCR1Job);

        // new job
        CondorJob bwaAlignR1Job = new CondorJob(String.format("%s_%d", BWAAlignCLI.class.getSimpleName(), ++count),
                null);
        graph.addVertex(bwaAlignR1Job);
        graph.addEdge(fastQCR1Job, bwaAlignR1Job);

        // new job
        CondorJob fastQCR2Job = new CondorJob(String.format("%s_%d", FastQCCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(fastQCR2Job);

        // new job
        CondorJob bwaAlignR2Job = new CondorJob(String.format("%s_%d", BWAAlignCLI.class.getSimpleName(), ++count),
                null);
        graph.addVertex(bwaAlignR2Job);
        graph.addEdge(fastQCR2Job, bwaAlignR2Job);

        // new job
        CondorJob bwaSAMPairedEndJob = new CondorJob(String.format("%s_%d", BWASAMPairedEndCLI.class.getSimpleName(),
                ++count), null);
        graph.addVertex(bwaSAMPairedEndJob);
        graph.addEdge(bwaAlignR1Job, bwaSAMPairedEndJob);
        graph.addEdge(bwaAlignR2Job, bwaSAMPairedEndJob);

        // new job
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJob(String.format("%s_%d",
                PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(bwaSAMPairedEndJob, picardAddOrReplaceReadGroupsJob);

        // new job
        CondorJob samtoolsIndexJob = new CondorJob(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(),
                ++count), null);
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, samtoolsIndexJob);

        VertexNameProvider<CondorJob> vnpId = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        VertexNameProvider<CondorJob> vnpLabel = new VertexNameProvider<CondorJob>() {
            @Override
            public String getVertexName(CondorJob job) {
                return job.getName();
            }
        };

        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(
                vnpId, vnpLabel, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "pipeline.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
