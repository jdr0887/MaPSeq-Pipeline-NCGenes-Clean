package edu.unc.mapseq.pipeline.ncgenes.clean;

import edu.unc.mapseq.pipeline.AbstractPipelineBeanService;

public class NCGenesCleanPipelineBeanService extends AbstractPipelineBeanService {

    private String referenceSequence;

    public NCGenesCleanPipelineBeanService() {
        super();
    }

    public String getReferenceSequence() {
        return referenceSequence;
    }

    public void setReferenceSequence(String referenceSequence) {
        this.referenceSequence = referenceSequence;
    }

}
