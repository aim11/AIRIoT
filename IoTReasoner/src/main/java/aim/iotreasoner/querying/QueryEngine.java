package aim.iotreasoner.querying;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;
import com.hp.hpl.jena.update.*;
import org.openrdf.repository.RepositoryConnection;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Created by amaarala on 12/10/13.
 */
public class QueryEngine {

    //private static IoTReasoner ioTReasoner = null;
    private Model dataModel;
    private String prefix = "obs";

    private RepositoryConnection con = null;
    private static String endpoint = "http://sesame-server:10010/openrdf-sesame/repositories/iot";

    private String dataFormat = "N3";

    public static void QueryEngine(){


    }
    public static void main(String[] args){

        /*ioTReasoner = new IoTReasoner();
        ioTReasoner.setRulesFile("file:./IoTSim/head_rules.txt");
        ioTReasoner.initialize();*/

       /* query();

        String[] ruleList =  {"Crossing", "HotSpot", "JamZone", "HighPollutionZone", "U-TurnZone"};

        for(String rule : ruleList){

            String[] r =  {rule};
            OntModel inferredModel = ioTReasoner.inferFromRules(r);

            final StringWriter wr = new StringWriter();

            System.out.println("------------------ INFERRED " + rule + " INSTANCES -------------------- ");
            inferredModel.write(System.out, "RDF/XML");

            //ioTReasoner.getSesameAdapter().addStatements(ioTReasoner.getSesameAdapter().getSesameConnection(), wr);
        }*/

        /*String qry = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "+
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "+"SELECT DISTINCT *  WHERE { ?s ?p ?o  }";
        OntModel model = getOntologyModel(query(qry));

        model.write(System.out, "RDF/XML");
        */



        /*System.out.println(" ----------------------------- Inferring datamodel - -----------" +(new Date()).toString());
        ioTReasoner.initDataModel(model);
        System.out.println(" ----------------------------- datamodel inferred - -----------" +(new Date()).toString());
        */


        //new ObservationDAOWrapper(model, graphURI, "LeftTurn");
        //List<Observation> obs = ObservationDAOWrapper.queryAll();

        /*IoTReasoner ioTReasoner = new IoTReasoner("/home/amaarala/thesis/dev/IoT/IoTSim/traffic.owl", "http://localhost/Schema/ontology#", "obs", "file:/home/amaarala/thesis/dev/IoT/IoTSim/traffic.rules");

        String[] classes = {"JamZone", "Crossing", "HotSpot", "HighPollutionZone", "UTurnZone"};

        Set<String> types = new HashSet<String>();
        //Parse rdf:types from rule body
        for(Rule rule : ioTReasoner.getRules()){

            for(int i=0; i<classes.length; i++){
                if(rule.getName().equals(classes[i])){
                    ClauseEntry[] body = rule.getBody();
                    String type1 = body[0].toString().split(" ")[2];
                    String type2 = body[1].toString().split(" ")[2];
                    types.add(type1);
                    types.add(type2);

                }
            }
        }

        String[] st = {types.toString()};
        System.out.println(st);       */
    }

    public static ResultSet query(String qry){

        Properties prop = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream("config.properties");
            prop.load(input);
            endpoint = prop.getProperty("sesame_endpoint");

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        ResultSet rs = null;

        try {

            Query query = QueryFactory.create(qry);
            QueryEngineHTTP qEngine = new QueryEngineHTTP(endpoint, query);
            //QueryExecution qe = QueryExecutionFactory.create(query,  ioTReasoner.inferFromRules())
            System.out.println(" ---------------------Querying DB --------------------- " + (new Date()).toString());

            QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, query);
            rs = qe.execSelect();

            //ResultSetFormatter.out(System.out, rs, query);
            System.out.println(" ---------------------DB queried--------------------- size: "+  rs.getRowNumber()+ " " + (new Date()).toString());
        } finally {
            //qe.close();
            //infModel.close();
            //model.close();
            //dataset.close();
        }
        return rs;
    }

    public void updateQuery(String qry){


        try {

            UpdateRequest query = UpdateFactory.create(qry);

            //QueryExecution qe = QueryExecutionFactory.create(query,  ioTReasoner.inferFromRules())
            //System.out.println(" ---------------------Updating DB --------------------- " + (new Date()).toString());

            //TODO: new execution method in 2.8.8
            //UpdateProcessor qEngine = UpdateExecutionFactory.create(query, endpoint+"/statements");
            //qEngine.execute();

            //ResultSetFormatter.out(System.out, rs, query);
            System.out.println(" ---------------------DB Updated--------------------- "+  (new Date()).toString());
        } finally {
            //qe.close();
            //infModel.close();
            //model.close();
            //dataset.close();
        }

    }


    public OntModel getOntologyModel(ResultSet rs, String prefix, String graphURI){

        OntModel tempModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
        tempModel.setNsPrefix(prefix, graphURI);
        //System.out.println(" - " + rs.toString());


        //set data to iotreasoner datamodel from sparql results and ini

        //tempModel.write(System.out, "RDF/XML");

        while(rs.hasNext()) {
            QuerySolution sol = rs.nextSolution();
            Resource resource = sol.getResource("s");
            RDFNode subject = sol.get("s");
            RDFNode pred = sol.get("p");
            RDFNode obj = sol.get("o");

            //System.out.println(" - " + sol.toString());

            //Individual jamsp1 = tempModel.createIndividual(graphURI + "JamSpeed1", instance );
            Property prop = subject.getModel().getProperty(pred.toString());
            //Property prop = tempModel.createProperty(pred.toString());
            tempModel.add(resource, prop, obj);

        }

        /*StmtIterator stmit = tempModel.listStatements();
        while(stmit.hasNext())         {
            System.out.println(" STMT - " + stmit.nextStatement().toString());
        }*/
        //final StringWriter wr = new StringWriter();

        return tempModel;

    }

    public String queryByType(String resourceUri, String prefix, List<String> typeList) {

        ResultSet rs = query(QueryBuilder.selectAllByType(resourceUri, (String[]) typeList.toArray(new String[0])));

        OntModel model = getOntologyModel(rs, prefix, resourceUri);
        StringWriter sw = new StringWriter();
        model.write(sw, dataFormat);

        return sw.toString();
    }

    public String inferByType(String resourceUri, String prefix, List<String> typeList) {

        //TODO:add all rules
        updateQuery(QueryBuilder.inferJam(resourceUri));

        //OntModel model = getOntologyModel(rs, prefix, resourceUri);
        //StringWriter sw = new StringWriter();
        //model.write(sw, dataFormat);

        return "";
    }


    public String deleteTempObs(String resourceUri) {

        updateQuery(QueryBuilder.deleteTempObservations(resourceUri));

        //OntModel model = getOntologyModel(rs, prefix, resourceUri);
        //StringWriter sw = new StringWriter();
        //model.write(sw, dataFormat);

        return "";
    }

    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    /*public static OntModel getOntologyModel(){
        OntModel tempModel;

        try {

            //String qry = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "+
            //        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "+"SELECT ?s ?p ?o  WHERE { ?s rdf:type <"+ graphURI+"Observation> }";
            String qry = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "+
                            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "+"SELECT ?s ?p ?o  WHERE { ?s ?p ?o }";

            Query query = QueryFactory.create(qry);
            QueryEngineHTTP qEngine = new QueryEngineHTTP(endpoint, query);
            //QueryExecution qe = QueryExecutionFactory.create(query,  ioTReasoner.inferFromRules())
            System.out.println(" ---------------------Querying DB --------------------- " + (new Date()).toString());

            QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, query);
            ResultSet rs = qe.execSelect();
            //ResultSetFormatter.out(System.out, rs, query);

            tempModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
            tempModel.setNsPrefix(prefix, graphURI);
            //System.out.println(" - " + rs.toString());

            while(rs.hasNext()) {
                QuerySolution sol = rs.nextSolution();
                //System.out.println(" - " + sol.toString());
                Resource resource = sol.getResource("s");
                RDFNode subject = sol.get("s");
                RDFNode pred = sol.get("p");
                RDFNode obj = sol.get("o");

                //Individual jamsp1 = tempModel.createIndividual(graphURI + "JamSpeed1", instance );
                Property prop = subject.getModel().getProperty(pred.toString());
                //Property prop = tempModel.createProperty(pred.toString());
                tempModel.add(resource, prop, obj);

            }
        } finally {
            //qe.close();
            //infModel.close();
            //model.close();
            //dataset.close();
        }
        //OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, tempModel);
        //ontmodel.addLoadedImport(ontology);
        //om.setNsPrefix(prefix, graphURI);

        tempModel.write(System.out, "RDF/XML");

        return tempModel;
    }*/




}
