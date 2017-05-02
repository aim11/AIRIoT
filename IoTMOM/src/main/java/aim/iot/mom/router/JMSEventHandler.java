package aim.iot.mom.router;

import aim.iotreasoner.IoTReasoner;
import com.hp.hpl.jena.rdf.model.Model;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;


/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 10.1.2014
 * Time: 12:03
 * To change this template use File | Settings | File Templates.
 */
public class JMSEventHandler implements Processor{

        private static final Logger LOGGER = LoggerFactory
        .getLogger(JMSEventHandler.class);

    private String owlFile = "./ontology.owl";
    private String ruleFile = ".rules_jena.txt";

    private long storageLatency=0;
    int count = 0;

    public void process(Exchange exchange) throws Exception {
            try {
                long reasoningLatency=0;

                Message message = (Message) exchange.getIn();

                //exchange.getIn().setBody(message.getBody());
                Date startd = new Date();
                IoTReasoner ioTReasoner = new IoTReasoner(owlFile, "http://localhost/SensorSchema/ontology#", "obs", ruleFile);

                //String[] splitted = message.getBody().toString().split("#&&##");
                //LOGGER.info("{}",message.getHeader("JMSCorrelationID"));
                ioTReasoner.setDataFormat("N3");
                ioTReasoner.createDataModel(message.getBody().toString());

                //Store instances
                //ioTReasoner.updateSesameRepository(ioTReasoner.getDataModel());
                String[] types = {""};
                Model model = ioTReasoner.inferModel(types, false);
                Date endd = new Date();
                reasoningLatency = reasoningLatency+(endd.getTime()-startd.getTime());

                if(!model.isEmpty())
                    ioTReasoner.updateSesameRepository(model);

                EventService.reasoninglatency = EventService.reasoninglatency+reasoningLatency;
                //EventService.count = EventService.count+1;
                System.out.println(String.valueOf(EventService.reasoninglatency));


            } catch (Throwable e) {
                e.printStackTrace();
                LOGGER.error("failed parsing JMS event {}", e.getMessage());
                exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            }

        }


}
