package aim.iot.iotnode;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.RDF;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Created by amaarala on 8/26/14.
 */
public class GPSObservationModel{

    String ontologyURI =  "http://localhost/Schema/ontology#";

    public SimpleDateFormat getSf() {
        return sf;
    }

    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public String getGraphIRI() {
        return graphIRI;
    }

    private String graphIRI = "http://localhost/Schema";
    private Model dataModel;

    public Resource getObsType() {
        return obsType;
    }

    public Property getDistanceType() {
        return distanceType;
    }

    public Property getIdType() {
        return idType;
    }

    public Property getLatType() {
        return latType;
    }

    public Property getLonType() {
        return lonType;
    }

    public Property getVelType() {
        return velType;
    }

    public Property getDirType() {
        return dirType;
    }

    public Property getSenderType() {
        return senderType;
    }

    public Property getAltType() {
        return altType;
    }

    public Property getDateType() {
        return dateType;
    }

    public Property getDateTimeType() {
        return dateTimeType;
    }

    private Resource obsType;
    private Property distanceType;
    private Property idType;
    private Property latType;
    private Property lonType;
    private Property velType;
    private Property dirType;
    private Property senderType;
    private Property altType;
    private Property dateType;
    private Property dateTimeType;

    public GPSObservationModel() {

        dataModel = ModelFactory.createDefaultModel();;

        obsType = dataModel.createResource(ontologyURI + "Observation");
        idType = dataModel.createProperty(ontologyURI + "hasID");
        latType = dataModel.createProperty(ontologyURI + "hasLatitude");
        lonType = dataModel.createProperty(ontologyURI + "hasLongitude");
        velType = dataModel.createProperty(ontologyURI + "hasVelocity");
        dirType = dataModel.createProperty(ontologyURI + "hasDirection");
        senderType = dataModel.createProperty(ontologyURI + "hasSender");
        distanceType = dataModel.createProperty(ontologyURI + "hasDistance");
        altType = dataModel.createProperty(ontologyURI + "hasAcceleration");
        dateType = dataModel.createProperty(ontologyURI + "hasDate");
        dateTimeType = dataModel.createProperty(ontologyURI + "hasDateTime");
        dataModel.setNsPrefix("obs", ontologyURI);
    }

    public Model addENtoDataModel(String enData) {

            String[] obs = enData.split(" ");
            //System.out.println(enData);
            UUID uuid = UUID.randomUUID();
            Resource obsInstance = dataModel.createResource(ontologyURI+"Observation_"+uuid.toString());

            dataModel.add(obsInstance, RDF.type, dataModel.createResource(this.getTemplate()[0]));

            /*for(int j = 0; j < obs.length; j++){
                dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[j+1]), dataModel.createLiteral(obs[j]));
            }*/

            //if(obs.length==13){

        if(obs.length==12){
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[1]), dataModel.createTypedLiteral(Integer.valueOf(obs[1])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[2]), dataModel.createTypedLiteral(Integer.valueOf(obs[2])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[3]), dataModel.createTypedLiteral(Double.valueOf(obs[3])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[4]), dataModel.createTypedLiteral(Double.valueOf(obs[4])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[5]), dataModel.createTypedLiteral(Double.valueOf(obs[5])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[6]), dataModel.createTypedLiteral(Integer.valueOf(obs[6])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[7]), dataModel.createTypedLiteral(Integer.valueOf(obs[7])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[8]), dataModel.createTypedLiteral(Double.valueOf(obs[8])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[9]), dataModel.createTypedLiteral(Double.valueOf(obs[9])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[10]), dataModel.createTypedLiteral(Long.valueOf(obs[10])));
            dataModel.add(obsInstance, dataModel.createProperty(this.getTemplate()[11]), dataModel.createLiteral(obs[11]));
        }

        return dataModel;
    }
    private String[] getTemplate(){
     /* String obsTemplate = "["+ontologyURI+"Observation "+ ontologyURI + "hasID "+ontologyURI + "hasArea "+ontologyURI +
              "hasLatitude "+ontologyURI + "hasLongitude "+ontologyURI + "hasVelocity "+ontologyURI + "hasDirection "+ontologyURI +
              "hasSender "+ontologyURI + "hasDistance "+ontologyURI + "hasAcceleration "+ontologyURI + "hasDate "+ontologyURI + "hasDateTime]";
*/
        String[] obsTemplate = {ontologyURI+"Observation", ontologyURI + "hasID", ontologyURI + "hasArea", ontologyURI +
                "hasLatitude", ontologyURI + "hasLongitude", ontologyURI + "hasVelocity", ontologyURI + "hasDirection", ontologyURI +
                "hasSender", ontologyURI + "hasDistance", ontologyURI + "hasAcceleration", ontologyURI + "hasDate", ontologyURI + "hasDateTime"};


        return obsTemplate;
    }

    public Model getDataModel() {
        return dataModel;
    }
}
