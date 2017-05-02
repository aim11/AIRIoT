package aim.iot.mom.router;

import aim.iotreasoner.IoTReasoner;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 10.1.2014
 * Time: 12:03
 * To change this template use File | Settings | File Templates.
 */
public class SensorPacketHandler implements Processor{

        private static final Logger LOGGER = LoggerFactory
        .getLogger(SensorPacketHandler.class);

        public void process(Exchange exchange) throws Exception {
            try {
                Message message = (Message) exchange.getIn();

                System.out.println("Received JMS message:" + message.getExchange().getIn().toString());

                String eventURN = (String) message.getHeader("urn");
                String eventType = (String) message.getExchange().getProperty("type");
                String endpointURI = exchange.getFromEndpoint().getEndpointUri();

                if (eventURN == null) {
                    System.out.println("eventURN not defined");
                }
                Map<String, Object> headers = message.getHeaders();
                Map<String, Object> properties = message.getExchange().getProperties();


                IoTReasoner ioTReasoner = new IoTReasoner();

                ioTReasoner.createDataModel(message.getBody().toString());

                //Store instances
                //ioTReasoner.updateSesameRepository(ioTReasoner.getDataModel());
                String[] types = {"RightTurn", "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop", "HighAcceleration", "HighDeacceleration"};
                ioTReasoner.inferModel(types, true);
                StringWriter str = new StringWriter();
                ioTReasoner.getInferredModel().write(str, "RDF/XML");

                System.out.println("REASONED DATA:\n" + str.toString());

                //exchange.getIn().setBody(message.getBody());

            } catch (Throwable e) {
                e.printStackTrace();
                LOGGER.error("failed parsing JMS event {}", e.getMessage());
                exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            }

        }
}
