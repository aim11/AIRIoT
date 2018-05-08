package aim.iot.mom.router;

import aim.iotreasoner.IoTReasoner;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: amaarala
 * Date: 10.1.2014
 * Time: 12:03
 * To change this template use File | Settings | File Templates.
 */
public class JsonLDEventHandler implements Processor{

        private static final Logger LOGGER = LoggerFactory
        .getLogger(JsonLDEventHandler.class);

        private String owlFile = "./traffic.owl";
        private String ruleFile = "./traffic.rules";

        String jsonHeader = "{\"@graph\" : [";
        private String ontologyURI ="http://localhost/Schema/ontology#";
    private String predictClasses;

    public void process(Exchange exchange) throws Exception {
            try {
                Message message = (Message) exchange.getIn();

                //System.out.println("Received JMS message:" + message.getExchange().getIn().toString());
                Properties prop = new Properties();
                InputStream input = null;

                try {
                    input = new FileInputStream("config.properties");
                    prop.load(input);
                    owlFile = prop.getProperty("owlfile");
                    ruleFile = prop.getProperty("rulesfile");
                    ontologyURI = prop.getProperty("ontologyuri");
                    predictClasses = prop.getProperty("predict_classnames");

                } catch (IOException ex) {
                    ex.printStackTrace();
                }

                IoTReasoner ioTReasoner = new IoTReasoner(owlFile, ontologyURI , "obs", ruleFile);
                ioTReasoner.initJsonLD();
                //String[] splitted = message.getBody().toString().split("#&&##");

                String jdata = this.jsonHeader + message.getBody().toString() + this.getContext();

                ioTReasoner.setDataFormat("JSON-LD");
                ioTReasoner.createDataModelJSONLD(jdata);

                //Store instances
                //ioTReasoner.updateSesameRepository(ioTReasoner.getDataModel());
                //String[] types = {"RightTurn", "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop", "HighAcceleration", "HighDeAcceleration", "VeryLongStop"};
                //String[] types = {"RightTurn", "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop"};
                String[] classes = predictClasses.split(",");
                ioTReasoner.inferModel(classes, true);

            } catch (Throwable e) {
                e.printStackTrace();
                LOGGER.error("failed parsing JMS event {}", e.getMessage());
                exchange.setProperty(Exchange.ROUTE_STOP, Boolean.TRUE);
            }

        }

    private String getContext(){

        String context = " ],\n" +
                "  \"@context\" : {\n" +
                "    \"hasDateTime\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasDateTime\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasLatitude\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasLatitude\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasDistance\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasDistance\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasArea\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasArea\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasSender\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasSender\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasDate\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasDate\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasID\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasID\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasDirection\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasDirection\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasVelocity\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasVelocity\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasAcceleration\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasAcceleration\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"hasLongitude\" : {\n" +
                "      \"@id\" : \"http://localhost/Schema/ontology#hasLongitude\",\n" +
                "      \"@type\" : \"@id\"\n" +
                "    },\n" +
                "    \"obs\" : \"http://localhost/Schema/ontology#\"\n" +
                "  }\n" +
                "}";

        return context;
    }
}
