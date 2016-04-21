package edu.unc.mapseq.messaging.ncgenes;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class NCGenesCleanMessageTest {

    @Test
    public void testQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616", "152.54.3.109"));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncgenes.clean");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            List<Integer> idList = Arrays.asList(27478, 48432, 52421, 86850, 113034, 113050, 113070, 139456, 189383, 196323, 234645, 333469,
                    400749, 503841, 544290, 1647894, 1774519, 1774531, 1774592, 1775335, 1784684, 1793789, 1800528, 1800765, 1801050,
                    1804460, 1804661, 1804837, 1804851, 1816082, 1816090, 1825231, 1825245, 1863415, 1863424, 1887301, 1917760, 1917792,
                    1918608, 1918750, 1918857, 1940349, 1940357, 1940368, 1963162, 2041808, 2053697, 2069150, 2069161, 2069174, 2081183,
                    2089400, 2089567, 2129669, 2129703, 2229201, 2229206, 2229212, 2229222, 2233707, 2258643, 2260973, 2260985, 2264668,
                    2270963, 2363034, 2363064, 2387775, 2392245, 2469829, 2469839, 2469849, 2469859, 2469892, 2469902, 2469912, 2494827,
                    2494828, 2494839, 2494885, 2506703, 2506717, 2506733, 2512563, 2512740, 2513091, 2513211, 2513580, 2513901, 2515995,
                    2516568, 2516580, 2516589, 2517333, 2517641, 2517651, 2517665, 2518386, 2518569, 2518786, 2519258, 2519471, 2521997,
                    2531077, 2531086, 2531462, 2531471, 2532151, 2532439, 2532449, 2532789, 2532954, 2534236, 2534246, 2534873, 2535675,
                    2535680, 2536090, 2536100, 2536703, 2536899);

            for (Integer id : idList) {

                StringWriter sw = new StringWriter();

                JsonGenerator generator = new JsonFactory().createGenerator(sw);

                generator.writeStartObject();
                generator.writeArrayFieldStart("entities");

                generator.writeStartObject();
                generator.writeStringField("entityType", "Flowcell");
                generator.writeStringField("id", id.toString());
                generator.writeEndObject();

                generator.writeStartObject();
                generator.writeStringField("entityType", "WorkflowRun");
                generator.writeStringField("name", String.format("CLEAN_%d", id));
                generator.writeEndObject();

                generator.writeEndArray();
                generator.writeEndObject();

                generator.flush();
                generator.close();

                sw.flush();
                sw.close();
                // System.out.println(sw.toString());
                producer.send(session.createTextMessage(sw.toString()));

            }

        } catch (IOException | JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

}
