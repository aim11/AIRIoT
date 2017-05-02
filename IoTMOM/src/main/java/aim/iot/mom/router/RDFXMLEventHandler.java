package aim.iot.mom.router;


import aim.iotreasoner.IoTReasoner;
import com.github.jsonldjava.jena.JenaJSONLD;
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
public class RDFXMLEventHandler implements Processor{

        private static final Logger LOGGER = LoggerFactory
        .getLogger(RDFXMLEventHandler.class);


    private long storageLatency = 0;
    private int count = 0;

    private String owlFile = "./traffic.owl";
    private String ruleFile = "./traffic.rules";


        public void process(Exchange exchange) throws Exception {
            try {

                long reasoningLatency = 0;

                JenaJSONLD.init();

                Message message = (Message) exchange.getIn();
                Date startd = new Date();

                IoTReasoner ioTReasoner = new IoTReasoner(owlFile, "http://localhost/SensorSchema/ontology#", "obs", ruleFile);

                //String[] splitted = message.getBody().toString().split("#&&##");

                ioTReasoner.setDataFormat("RDF/XML");
                String msg = "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" xmlns:obs=\"http://localhost/SensorSchema/ontology#\">"+message.getBody().toString()+"</rdf:RDF>";
                System.out.println(msg);

                ioTReasoner.createDataModel(msg);

                //Store instances
                //ioTReasoner.updateSesameRepository(ioTReasoner.getDataModel());
                String[] types = {};
                Model model = ioTReasoner.inferModel(types, false);

                System.out.println("Reasoned: ");
                model.write(System.out);
                Date endd = new Date();
                reasoningLatency = endd.getTime()-startd.getTime();

                Date d2 = new Date();
                if(!model.isEmpty())
                    ioTReasoner.updateSesameRepository(model);
                Date d22 = new Date();

                EventService.reasoninglatency = EventService.reasoninglatency+reasoningLatency;

            } catch (Throwable e) {
                e.printStackTrace();
                LOGGER.error("failed parsing JMS event {}", e.getMessage());
                exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            }

        }

}
