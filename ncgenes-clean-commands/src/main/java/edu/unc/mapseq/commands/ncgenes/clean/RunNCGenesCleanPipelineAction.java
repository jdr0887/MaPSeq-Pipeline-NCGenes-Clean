package edu.unc.mapseq.commands.ncgenes.clean;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBean;

@Command(scope = "mapseq", name = "run-ncgenes-clean-pipeline", description = "Run NCGenes CleanPipeline")
public class RunNCGenesCleanPipelineAction extends AbstractAction {

    @Argument(index = 0, name = "sequencerRunId", description = "SequencerRun.id", required = true, multiValued = false)
    private Long sequencerRunId;

    @Argument(index = 1, name = "workflowRunName", description = "WorkflowRun.name", required = true, multiValued = false)
    private String workflowRunName;

    private MaPSeqDAOBean mapseqDAOBean;

    private MaPSeqConfigurationService mapseqConfigurationService;

    public RunNCGenesCleanPipelineAction() {
        super();
    }

    @Override
    public Object doExecute() {

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                mapseqConfigurationService.getWebServiceHost("localhost")));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncgenes.clean");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            String format = "{\"account_name\":\"%s\",\"entities\":[{\"entity_type\":\"SequencerRun\",\"id\":\"%d\"},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s\"}]}";
            producer.send(session.createTextMessage(String.format(format, System.getProperty("user.name"),
                    sequencerRunId, workflowRunName)));
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }

    public MaPSeqDAOBean getMapseqDAOBean() {
        return mapseqDAOBean;
    }

    public void setMapseqDAOBean(MaPSeqDAOBean mapseqDAOBean) {
        this.mapseqDAOBean = mapseqDAOBean;
    }

    public MaPSeqConfigurationService getMapseqConfigurationService() {
        return mapseqConfigurationService;
    }

    public void setMapseqConfigurationService(MaPSeqConfigurationService mapseqConfigurationService) {
        this.mapseqConfigurationService = mapseqConfigurationService;
    }

}
