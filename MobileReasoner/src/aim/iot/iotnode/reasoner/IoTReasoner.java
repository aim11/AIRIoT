package aim.iot.iotnode.reasoner;

/**
 * Created by amaarala on 8/22/14.
 */

import android.content.res.AssetManager;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;


import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.OntProperty;
import com.hp.hpl.jena.ontology.OntResource;
import com.hp.hpl.jena.rdf.model.InfModel;
import com.hp.hpl.jena.rdf.model.Model;

import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.reasoner.Reasoner;
import com.hp.hpl.jena.reasoner.rulesys.ClauseEntry;
import com.hp.hpl.jena.reasoner.rulesys.GenericRuleReasoner;
import com.hp.hpl.jena.reasoner.rulesys.Rule;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.PrintUtil;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.vocabulary.RDF;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.jena.riot.RDFDataMgr;

/*if(iotreasoner == null){



import java.io.*;
import java.util.*;

/**
* Created with IntelliJ IDEA.
* User: amaarala
* Date: 26.11.2013
* Time: 16:47
* To change this template use File | Settings | File Templates.
*/
public class IoTReasoner {

    private String ontology = "android.resource://aim.iot.iotnode/raw/traffic.owl";
    private String graphURI = "http://localhost/Schema/ontology#";
    private String prefix = "obs";
    private String rulesFile = "./traffic.rules";

    Logger logger = LoggerFactory.getLogger(IoTReasoner.class);

    private InfModel rulesmodel = null;

    private String dataFormat = "RDF/XML";

    public Model dataModel = null;
    private OntModel ontologyModel = null;

    //private static IoTReasoner iotreasoner = null;
    private Model schema = null;
    private OntModel ontSchema = null;

    private Reasoner ruleReasoner = null;


    //private OntModel ontmodel = null;


    private Model collectedInfModel;
    private List rules = new ArrayList<Rule>();
    private InputStream inputOntology;
    private InputStream inputRules;

    public Model getDeductionsModel() {
        return deductionsModel;
    }

    private Model deductionsModel;

    public IoTReasoner() {
        /*if(iotreasoner == null){
            iotreasoner = this;
        }*/
        //schema = ModelFactory.createDefaultModel();
        schema = ModelFactory.createDefaultModel();

        // load data into model
        //FileManager.get().readModel( schema, ontology );


       PrintUtil.registerPrefix(prefix, graphURI);

    }



/*

    public IoTReasoner(String ontology, String graphURI, String prefix, String rulesFile) {
        this.setRulesFile(rulesFile);
        this.setPrefix(prefix);
        this.setGraphURI(graphURI);
        this.setOntology(ontology);

        schema = ModelFactory.createDefaultModel();

        // load data into model
        //FileManager.get().readModel( this.dataModel, "observations.rdf" );
        FileManager.get().readModel( schema, ontology );

        PrintUtil.registerPrefix(prefix, graphURI);

        this.initializeGenericReasoner();

    }

    public IoTReasoner(String rulesFile) {
        this.setRulesFile(rulesFile);

        schema = ModelFactory.createDefaultModel();

        // load data into model
        //FileManager.get().readModel( this.dataModel, "observations.rdf" );
        FileManager.get().readModel( schema, ontology );

        PrintUtil.registerPrefix(prefix, graphURI);

        this.initializeGenericReasoner();

    }*/

    /*public void initJsonLD(){
        JenaJSONLD.init();
    }*/


   /* public IoTReasoner(List<Rule> rl) {


        this.rules = rl;

        schema = ModelFactory.createDefaultModel();

        FileManager.get().readModel( schema, ontology );

        PrintUtil.registerPrefix(prefix, graphURI);

        this.initializeGenericReasoner();
    }*/

    public void initializeGenericReasoner(String ontology, String rules){


        //Reasoner iotreasoner = ReasonerRegistry.getOWLReasoner();
        //FileManager f = new FileManager();
        //f.readModel(schema, ontology);


        /*if(ontology != null){

            ontSchema = ModelFactory.createOntologyModel();
            ontSchema.read(ontology, graphURI, "RDF/XML"); }
        else {
            throw new NullPointerException("No inputOntology given!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }*/

        ruleReasoner = new GenericRuleReasoner(getRules(rules));

        //LPBackwardRuleReasoner lpbrr = new LPBackwardRuleReasoner(getRules());

        ruleReasoner.bindSchema(schema);


    }


    public OntModel getInferredModel() {
        return ontologyModel;
    }

    public void setGraphURI(String graphURI) {
        this.graphURI = graphURI;
    }

    public void setRulesmodel(InfModel rulesmodel) {
        this.rulesmodel = rulesmodel;
    }


    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public  Reasoner getRuleReasoner() {
        return ruleReasoner;
    }

    public  void setRuleReasoner(Reasoner ruleReasoner) {
        this.ruleReasoner = ruleReasoner;
        ruleReasoner.bindSchema(schema);
    }
    public String getRulesFile() {
        return rulesFile;
    }

    public void setRulesFile(String rulesFile) {
        this.rulesFile = rulesFile;
    }



    public void updateSesameRepository(Model model)  {

        // creates a new, empty in-memory model
       /* RemoteRepositoryManager manager = new RemoteRepositoryManager(sesameServer);
        try {
            manager.initialize();
        } catch (RepositoryException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }     */
        // creates a new, empty in-memory model

        //Resource ENTITY_TYPE = rulesmodel.getResource("http://localhost/Schema/ontology#Jam");
        /*StmtIterator it = rulesmodel.getDeductionsModel().listStatements(null, RDF.type, ENTITY_TYPE);

        while (it.hasNext()) {
            System.out.println(" - " + PrintUtil.print(it.nextStatement().toString()));
            //addStatement(getSesameConnection(), it.nextStatement());
        }*/

        //write statements as RDF/XML
        final StringWriter wr = new StringWriter();
        //inferredModel.write(System.out, dataFormat);

        model.write(wr, null, dataFormat);

        //Add statements to RDF DB

        //getSesameAdapter().addStatements(getSesameAdapter().createConnection(), wr);

        //rulesmodel.write(System.out, dataFormat);

        //showModelSize(this.dataModel);


    }

   /* public Model inferQTModel(String[] classes){

        //TODO: examine if there is way to implement kinda backward chained inference
        // where we do not need to query body rules beforehand
        Set<String> types = new HashSet<String>();
        //Parse rdf:types from rule body
        for(Rule rule : getRules()){

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



        String query = QueryBuilder.selectAllByType(graphURI, types.toArray(new String[0]));
        ResultSet rs = QueryEngine.query(query);



        this.dataModel = getOntologyModel(rs, prefix, graphURI);

        Model model = inferModel(classes, true);

        return model;

    }*/

   /* public void inferQTSpatial(String[] classes){

        //TODO: examine if there is way to implement kinda backward chained inference
        // where we do not need to query body rules beforehand

        String query = QueryBuilder.selectAllByType(graphURI, classes);
        ResultSet rs = QueryEngine.query(query);

        this.dataModel = getOntologyModel(rs, prefix, graphURI);

        inferModel(classes, true);

    }*/

    public Model inferModel(){

       // String[] classes = {"LowSpeed", "RightTurn",  "LeftTurn", "UTurn", "Jam", "HighAvgSpeed", "LongStop", "HighAcceleration", "HighDeAcceleration", "VeryLongStop"};
        //String[] classes = {"RightTurn", "LeftTurn", "UTurn", "Jam",  "LongStop"};


        //Inference
        InfModel infModel = inferFromRules();
        //infModel.write(System.out);

        //List<String> interestingEvents = Arrays.asList(classes);

        //OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, infModel);

        //LOGGER.info(" --------- inferred --------- "+((new Date()).getTime()-time));

        //OntModel newKnowledgeModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, ModelFactory.createDefaultModel());
        //newKnowledgeModel.setNsPrefix(prefix, graphURI);

        //Model inferencedInstancesModel = ModelFactory.createDefaultModel();
        //inferencedInstancesModel.setNsPrefixes(infModel.getNsPrefixMap());
        Model inferencedInstancesModel = infModel.getDeductionsModel();
        /*
        //TESTING all rules
        Set<String> types = new HashSet<String>();
        //Parse rdf:types from rule body
        for(Rule rule : getRules()){

            types.add(rule.getName());


        }*/


    //TODO: REMOVE THIS, ADD wanted properties inside rules
        /*for(String classType : interestingEvents) {
            //Property p = dataModel.getProperty(graphURI, "rdf:type");

            //This takes much longer with backward rules
            //TODO: optimize for backward rules
            OntClass oc = om.getOntClass(graphURI + classType);

            if(oc != null){
                ExtendedIterator<? extends OntResource> instances = oc.listInstances();
                ExtendedIterator<? extends OntProperty> propit = oc.listDeclaredProperties();
                ArrayList<String> proplist = new ArrayList<String>();

                while (propit.hasNext()) {

                    proplist.add(propit.next().toString());
                }
                while (instances.hasNext()) {

                    OntResource instance = instances.next();

                    //Individual jamsp1 = tempModel.createIndividual(graphURI + "JamSpeed1", instance );
                   
                    inferencedInstancesModel.add(instance, RDF.type, om.getResource(graphURI + classType));
                    System.out.println(" ---------Inferred type--------- " + classType);

                    for (String prop : proplist) {
                        //addStatement(getSesameConnection(), it.nextStatement());
                        try{
                            if(instance.getProperty(om.getOntProperty(prop))!=null){
                                System.out.println(prop + ": " + instance.getProperty(om.getOntProperty(prop)).getString());

                                inferencedInstancesModel.add(instance, om.getOntProperty(prop), instance.getProperty(om.getOntProperty(prop)).getString());
                            }
                        }catch(NoSuchElementException e){

                        }
                    }

                    //TODO: try adding one statement at time
                    //updateSesameRepository(ioTReasoner.getInferredModel());
                }

            }

        }*/

		//inferencedInstancesModel.write(System.out);
        //if(store == true && !inferencedInstancesModel.isEmpty())
        //    this.updateSesameRepository(inferencedInstancesModel);

        //tempModel.write(System.out);
        //this.ontologyModel = inferencedInstancesModel;

        return inferencedInstancesModel;

    }


    public InfModel inferFromRules() {

        long time = (new Date()).getTime();
        //System.out.println(" ---------Infer --------- "+ time);

        //Build an inferred model by attaching the given RDF model to the given reasoner. This form of the call allows two data sets to be merged and reasoned over - conventionally one contains schema data and one instance data but this is not a formal requirement.
        //this.rulesmodel = ModelFactory.createInfModel(ruleReasoner, schema, dataModel);
        this.rulesmodel = ModelFactory.createInfModel(ruleReasoner, dataModel);

        //System.out.println(" ---------inferred 1--------- "+((new Date()).getTime()-time));
        this.rulesmodel.setNsPrefix(prefix, graphURI);

        //Deductions model not created from backward rules
        //to get only deducted triples strictly from the model,

        rulesmodel.getDeductionsModel().setNsPrefix("obs", graphURI);
        //this.deductionsModel = rulesmodel.getDeductionsModel();


        return rulesmodel;
    }

    public InfModel inferFromRule(List<Rule> rules) {

        long time = (new Date()).getTime();
        //System.out.println(" ---------Infer --------- "+ time);

        GenericRuleReasoner reasoner = new GenericRuleReasoner(rules);
        reasoner.bindSchema(schema);

        //LPBackwardRuleReasoner lpbrr = new LPBackwardRuleReasoner(getRules());

        InfModel infModel = ModelFactory.createInfModel(reasoner, dataModel);

        //System.out.println(" ---------inferred 1--------- "+((new Date()).getTime()-time));
        infModel.setNsPrefix(prefix, graphURI);

        //Deductions model not created from backward rules
        //to get only deducted triples strictly from the model,
        infModel.getDeductionsModel().setNsPrefix("obs", graphURI);

        //updateSesameRepository(rulesmodel.getDeductionsModel());

        //OntModel om = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RULE_INF, infModel.getDeductionsModel());

        //LOGGER.info(" --------- inferred --------- "+((new Date()).getTime()-time));


        return infModel;
    }



    /*protected void printStatements(Model m, Resource s, Property p, Resource o) {
        for (StmtIterator i = m.listStatements(s,p,o); i.hasNext(); ) {
            Statement stmt = i.nextStatement();
            System.out.println(" - " + PrintUtil.print(stmt));
        }
    }*/

    protected void showModelSize( Model m ) {
        System.out.println( String.format( "The model contains %d triples", m.size() ) );
    }

    /*public static IoTReasoner getReasoner() {
        return this;
    }*/

    public List<Rule> getRules(){
        //GPSObservation(?o), hasVelocity(?o, ?v), lessThan(?v, "5"^^int) -> Stop1(?o)
        //String stop1 =   "[stop1: (?d rdf:type obs:Observation)(?d obs:hasVelocity ?v) lessThan(?v,'5'^^xsd:integer) -> (?d rdf:type obs:Stop1)]";
        //GPSObservation(?o), Stop1(?j1), hasID(?j1, ?ID1), hasID(?o, ?ID2), hasSender(?j1, ?s), hasSender(?o, ?s), hasVelocity(?o, ?v), add(?ID2, ?ID1, "1"^^int), lessThan(?v, 3) -> Stop2(?o)
        List<Rule> rules = new ArrayList<Rule>();
        if(this.rules.isEmpty() && this.rulesFile != null) {
            //rules = Rule.rulesFromURL(this.rulesFile);

            try {

                BufferedReader br = new BufferedReader(new InputStreamReader(inputRules, "UTF-8"));
                String line = null;
                String str ="";
                try {
                    while (( line = br.readLine()) != null) {
                        str = str +System.getProperty ("line.separator")+ line;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                rules = Rule.parseRules(str);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            this.rules = rules;

        }
        if(this.rules.isEmpty() || this.rules == null )
            throw new NullPointerException("No rules given!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        /*Map<String, List<Rule>> rulez = new HashMap<String, List<Rule>>();

        for (Rule rule : rules) {
            if(rulez.containsKey(rule.getName()))
                rulez.get(rule.getName()).add(rule);
            else {
                List<Rule> rl = new ArrayList<Rule>();
                rl.add(rule);
                rulez.put(rule.getName(), rl);
            }
        }*/

        return this.rules;
    }

    public List<Rule> getRules(String rulesString){
        //GPSObservation(?o), hasVelocity(?o, ?v), lessThan(?v, "5"^^int) -> Stop1(?o)
        //String stop1 =   "[stop1: (?d rdf:type obs:Observation)(?d obs:hasVelocity ?v) lessThan(?v,'5'^^xsd:integer) -> (?d rdf:type obs:Stop1)]";
        //GPSObservation(?o), Stop1(?j1), hasID(?j1, ?ID1), hasID(?o, ?ID2), hasSender(?j1, ?s), hasSender(?o, ?s), hasVelocity(?o, ?v), add(?ID2, ?ID1, "1"^^int), lessThan(?v, 3) -> Stop2(?o)
        List<Rule> rules = new ArrayList<Rule>();
        if(this.rules.isEmpty() && this.rulesFile != null) {
            //rules = Rule.rulesFromURL(this.rulesFile);
             rules = Rule.parseRules(rulesString);
            this.rules = rules;

        }
        if(this.rules.isEmpty() || this.rules == null )
            throw new NullPointerException("No rules given!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        /*Map<String, List<Rule>> rulez = new HashMap<String, List<Rule>>();

        for (Rule rule : rules) {
            if(rulez.containsKey(rule.getName()))
                rulez.get(rule.getName()).add(rule);
            else {
                List<Rule> rl = new ArrayList<Rule>();
                rl.add(rule);
                rulez.put(rule.getName(), rl);
            }
        }*/

        return this.rules;
    }



    public Model getDataModel() {
        return dataModel;
    }

    public Model getSchema() {
        return schema;
    }

    public void initDataModel(Model dataModel) {
        this.dataModel = dataModel;
        //inferFromRules();
        //initOntmodel();
    }
    public void setOntology(String ontology) {
        this.ontology = ontology;
    }



    /*public void createDataModel(String rdfData) {

        Model model = ModelFactory.createDefaultModel();
        StringReader reader = new StringReader(rdfData);
        try{
            model.read(reader, null, dataFormat);
        }catch(org.apache.jena.riot.RiotException e) {
            e.printStackTrace();
        }
        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();

    }

    public void createDataModelJSONLD(String rdfData) {

        Model model = ModelFactory.createDefaultModel();
        //StringReader reader = new StringReader(rdfData);
        try{
            InputStream inStream = new ByteArrayInputStream(rdfData.getBytes("UTF-8"));
            RDFDataMgr.read(model, inStream, graphURI, JenaJSONLD.JSONLD);
        }catch(org.apache.jena.riot.RiotException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();

    }*/


    public Model createDataModel(InputStream rdfData) {

        Model model = ModelFactory.createDefaultModel();

        model.read(rdfData, null);

        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();
        return model;

    }

   /* public  void createDataModel(String rdfData) {

        Model model = ModelFactory.createDefaultModel();

        model.read(rdfData, null);

        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();

    }*/

    public void createDataModel(String rdfData) {

        Model model = ModelFactory.createDefaultModel();
        StringReader reader = new StringReader(rdfData);

            model.read(reader, null, dataFormat);

        this.dataModel = model;

        //inferFromRules();
        //initOntmodel();

    }


    public void createDataModel(String[] splitted) {
        Model model = ModelFactory.createDefaultModel();
        for(int i = 0; i<splitted.length; i++) {
            StringReader reader = new StringReader(splitted[i]);
            model.read(reader, null, dataFormat);
        }
        this.dataModel = model;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    public void setDataModel(Model dataModel) {
        this.dataModel = dataModel;
    }

    public void setOntology(InputStream isOnt) {
        this.inputOntology = isOnt;
    }

    public void setRules(InputStream rules) {
        this.inputRules = rules;
    }
}

