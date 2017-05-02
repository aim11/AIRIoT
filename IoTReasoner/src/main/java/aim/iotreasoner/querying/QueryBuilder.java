package aim.iotreasoner.querying;

/**
 * Created by amaarala on 12/19/13.
 */
public class QueryBuilder {

    public static String selectAllByType(String resourceUri, String[] classes){

        int len = classes.length;
        String whereClause = "";
        for(int i = 0; i<len; i++){

            whereClause += "{ ?s rdf:type <"+resourceUri + classes[i] +"> }";
            if( i < (len-1))
                whereClause += "UNION";

        }


        String qry = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "+
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "+"SELECT ?s ?p ?o  WHERE { " + whereClause + ". ?s ?p ?o  }";
        if (len==0)
            qry =  "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "+
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>  "+"SELECT ?s ?p ?o  WHERE { ?s ?p ?o  }";

        return qry;
    }

    public static String insertData(String rdfData){

        String query = "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <http://localhost/SensorSchema/ontology#>\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
                "INSERT DATA";

        return query;
    }

    public static String inferJam(String resourceUri){


        String qry = "\n" +
                "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <http://localhost/SensorSchema/ontology#>\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "INSERT { GRAPH <http://localhost/SensorSchema/inferred> {?obs1 rdf:type obs:Jam}  } \n" +
                "WHERE\n" +
                "  {  GRAPH <http://localhost/SensorSchema/temp> {" +
                "   ?obs1 rdf:type obs:Observation .\n" +
                "    ?obs1 obs:hasVelocity ?v3 .\n" +
                "    ?obs1 obs:hasSender ?snd1 .\n" +
                "    ?obs1 obs:hasID ?ID3 .\n" +
                "    ?obs1 obs:hasDate ?date3.\n" +
                "    FILTER(?ID3 = (?ID2 + 1))\n" +
                "    FILTER ( ?v3 > 0 && ?v3 < \"25\"^^xsd:double )\n" +
                "    { SELECT ?v2 ?v1 ?date1 ?ID2 ?snd1\n" +
                "      WHERE\n" +
                "        { ?obs1 obs:hasVelocity ?v2 .\n" +
                "          ?obs1 obs:hasSender ?snd1 .\n" +
                "          ?obs1 obs:hasID ?ID2.            \n" +
                "\t  FILTER(?ID2 = (?ID1 + 1))\n" +
                "          FILTER ( ?v2 > 0 && ?v2 < \"25\"^^xsd:double ) \n" +
                "\t    { SELECT ?v1 ?date1 ?ID1 ?snd1\n" +
                "\t      WHERE\n" +
                "\t\t{ ?obs1 obs:hasVelocity ?v1 .\n" +
                "\t\t  ?obs1 obs:hasSender ?snd1 .\n" +
                "\t\t  ?obs1 obs:hasID ?ID1.  \n" +
                "\t\t  ?obs1 obs:hasDate ?date1. \n" +
                "                  FILTER ( ?v1 > 0 && ?v1 < \"25\"^^xsd:double ) \n" +
                "\t\t}\n" +
                "\t    }        \n" +
                "        }\n" +
                "    }\n" +
                "    BIND (?date3 - ?date1 AS ?time)\n" +
                "    BIND(?v1 + ?v2 + ?v3 AS ?vs)    \n" +
                "    FILTER(?vs < 60 && ?time > 60000 && ?time < 300000)\n" +
                "    \n" +
                "\n" +
                "  }}";

        return qry;
    }

    public static String inferJam2(String resourceUri){


        String qry = "\n" +
                "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <http://localhost/SensorSchema/ontology#>\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "INSERT { GRAPH <http://localhost/SensorSchema/ontology#> {?obs1 rdf:type obs:Jam}  } \n" +
                "WHERE\n" +
                "  {  GRAPH <http://localhost/SensorSchema/"+resourceUri+"> {" +
                "   ?obs1 rdf:type obs:Observation .\n" +
                "    ?obs1 obs:hasVelocity ?v3 .\n" +
                "    ?obs1 obs:hasSender ?snd1 .\n" +
                "    ?obs1 obs:hasID ?ID3 .\n" +
                "    ?obs1 obs:hasDate ?date3.\n" +
                "    FILTER(?ID3 = (?ID2 + 1))\n" +
                "    FILTER ( ?v3 > 0 && ?v3 < \"25\"^^xsd:double )\n" +
                "    { SELECT ?v2 ?v1 ?date1 ?ID2 ?snd1\n" +
                "      WHERE\n" +
                "        { ?obs1 obs:hasVelocity ?v2 .\n" +
                "          ?obs1 obs:hasSender ?snd1 .\n" +
                "          ?obs1 obs:hasID ?ID2.            \n" +
                "\t  FILTER(?ID2 = (?ID1 + 1))\n" +
                "          FILTER ( ?v2 > 0 && ?v2 < \"25\"^^xsd:double ) \n" +
                "\t    { SELECT ?v1 ?date1 ?ID1 ?snd1\n" +
                "\t      WHERE\n" +
                "\t\t{ ?obs1 obs:hasVelocity ?v1 .\n" +
                "\t\t  ?obs1 obs:hasSender ?snd1 .\n" +
                "\t\t  ?obs1 obs:hasID ?ID1.  \n" +
                "\t\t  ?obs1 obs:hasDate ?date1. \n" +
                "                  FILTER ( ?v1 > 0 && ?v1 < \"25\"^^xsd:double ) \n" +
                "\t\t}\n" +
                "\t    }        \n" +
                "        }\n" +
                "    }\n" +
                "    BIND (?date3 - ?date1 AS ?time)\n" +
                "    BIND(?v1 + ?v2 + ?v3 AS ?vs)    \n" +
                "    FILTER(?vs < 60 && ?time > 60000 && ?time < 300000)\n" +
                "    \n" +
                "\n" +
                "  }}";

        return qry;
    }

    public static String inferLongStop(String s){
        String query = "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <http://localhost/SensorSchema/ontology#>\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "INSERT { GRAPH <http://localhost/SensorSchema/inferred> {?obs1 rdf:type obs:LongStop }}  \n" +
                "WHERE\n" +
                "  { \n" +
                "    GRAPH <http://localhost/SensorSchema/temp> {" +
                "   ?obs1 rdf:type obs:Observation .\n" +
                "    ?obs1 obs:hasVelocity ?v3 .\n" +
                "    ?obs1 obs:hasSender ?snd1 .\n" +
                "    ?obs1 obs:hasID ?ID3 .\n" +
                "    ?obs1 obs:hasDate ?date3.\n" +
                "    FILTER(?ID3 = (?ID2 + 1))\n" +
                "    FILTER ( ?v3 > 0 && ?v3 < \"3\"^^xsd:double )\n" +
                "    { SELECT ?v2 ?v1 ?date1 ?ID2 ?snd1\n" +
                "      WHERE\n" +
                "        { ?obs1 obs:hasVelocity ?v2 .\n" +
                "          ?obs1 obs:hasSender ?snd1 .\n" +
                "          ?obs1 obs:hasID ?ID2.            \n" +
                "\t  FILTER(?ID2 = (?ID1 + 1))\n" +
                "          FILTER ( ?v2 > 0 && ?v2 < \"3\"^^xsd:double ) \n" +
                "\t    { SELECT ?v1 ?date1 ?ID1 ?snd1\n" +
                "\t      WHERE\n" +
                "\t\t{ ?obs1 obs:hasVelocity ?v1 .\n" +
                "\t\t  ?obs1 obs:hasSender ?snd1 .\n" +
                "\t\t  ?obs1 obs:hasID ?ID1.  \n" +
                "\t\t  ?obs1 obs:hasDate ?date1. \n" +
                "                  FILTER ( ?v1 > 0 && ?v1 < \"3\"^^xsd:double ) \n" +
                "\t\t\t\n" +
                "\t\t}\n" +
                "\t    }        \n" +
                "        }\n" +
                "    }\n" +
                "    BIND (?date3 - ?date1 AS ?time)\n" +
                "    BIND(?v1 + ?v2 + ?v3 AS ?vs)    \n" +
                "    FILTER(?vs < 3 && ?time > 60000)\n" +
                "    \n" +
                "\n" +
                "  }}";
        return query;
    }

    public static String inferLongStop2(String s){
        String query = "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <http://localhost/SensorSchema/ontology#>\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "INSERT { GRAPH <http://localhost/SensorSchema/ontology#> {?obs1 rdf:type obs:LongStop }}  \n" +
                "WHERE\n" +
                "  { \n" +
                "    GRAPH <"+s+"> {" +
                "   ?obs1 rdf:type obs:Observation .\n" +
                "    ?obs1 obs:hasVelocity ?v3 .\n" +
                "    ?obs1 obs:hasSender ?snd1 .\n" +
                "    ?obs1 obs:hasID ?ID3 .\n" +
                "    ?obs1 obs:hasDate ?date3.\n" +
                "    FILTER(?ID3 = (?ID2 + 1))\n" +
                "    FILTER ( ?v3 > 0 && ?v3 < \"3\"^^xsd:double )\n" +
                "    { SELECT ?v2 ?v1 ?date1 ?ID2 ?snd1\n" +
                "      WHERE\n" +
                "        { ?obs1 obs:hasVelocity ?v2 .\n" +
                "          ?obs1 obs:hasSender ?snd1 .\n" +
                "          ?obs1 obs:hasID ?ID2.            \n" +
                "\t  FILTER(?ID2 = (?ID1 + 1))\n" +
                "          FILTER ( ?v2 > 0 && ?v2 < \"3\"^^xsd:double ) \n" +
                "\t    { SELECT ?v1 ?date1 ?ID1 ?snd1\n" +
                "\t      WHERE\n" +
                "\t\t{ ?obs1 obs:hasVelocity ?v1 .\n" +
                "\t\t  ?obs1 obs:hasSender ?snd1 .\n" +
                "\t\t  ?obs1 obs:hasID ?ID1.  \n" +
                "\t\t  ?obs1 obs:hasDate ?date1. \n" +
                "                  FILTER ( ?v1 > 0 && ?v1 < \"3\"^^xsd:double ) \n" +
                "\t\t\t\n" +
                "\t\t}\n" +
                "\t    }        \n" +
                "        }\n" +
                "    }\n" +
                "    BIND (?date3 - ?date1 AS ?time)\n" +
                "    BIND(?v1 + ?v2 + ?v3 AS ?vs)    \n" +
                "    FILTER(?vs < 3 && ?time > 60000)\n" +
                "    \n" +
                "\n" +
                "  }}";
        return query;
    }

    public static String inferHighAvgSpeed(String ruri){

        String query = "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <http://localhost/SensorSchema/ontology#>\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "INSERT { GRAPH <http://localhost/SensorSchema/inferred> { ?obs1 rdf:type obs:HighAvgSpeed }}  \n" +
                "WHERE\n" +
                "  { \n" +
                "   GRAPH <http://localhost/SensorSchema/temp> {" +
                "   ?obs1 rdf:type obs:Observation .\n" +
                "    ?obs1 obs:hasVelocity ?v3 .\n" +
                "    ?obs1 obs:hasSender ?snd1 .\n" +
                "    ?obs1 obs:hasID ?ID3 .\n" +
                "    ?obs1 obs:hasDate ?date3.\n" +
                "    FILTER(?ID3 = (?ID2 + 1))\n" +
                "    FILTER ( ?v3 > 80 )\n" +
                "    { SELECT ?v2 ?v1 ?date1 ?ID2 ?snd1\n" +
                "      WHERE\n" +
                "        { ?obs1 obs:hasVelocity ?v2 .\n" +
                "          ?obs1 obs:hasSender ?snd1 .\n" +
                "          ?obs1 obs:hasID ?ID2.            \n" +
                "\t  FILTER(?ID2 = (?ID1 + 1))\n" +
                "          FILTER ( ?v2 > 80 ) \n" +
                "\t    { SELECT ?v1 ?date1 ?ID1 ?snd1\n" +
                "\t      WHERE\n" +
                "\t\t{ ?obs1 obs:hasVelocity ?v1 .\n" +
                "\t\t  ?obs1 obs:hasSender ?snd1 .\n" +
                "\t\t  ?obs1 obs:hasID ?ID1.  \n" +
                "\t\t  ?obs1 obs:hasDate ?date1. \n" +
                "                  FILTER ( ?v1 > 80 ) \n" +
                "\t\t\t\n" +
                "\t\t}\n" +
                "\t    }        \n" +
                "        }\n" +
                "    }  \n" +
                "      \n" +
                "    BIND(?v1 + ?v2 + ?v3 AS ?vs)  \n" +
                "    FILTER(?vs > 240)      \n" +
                "\n" +
                "  }}";
        return query;
    }

    public static String inferHighAvgSpeed2(String ruri){

        String query = "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <http://localhost/SensorSchema/ontology#>\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "INSERT { GRAPH <http://localhost/SensorSchema/ontology#> { ?obs1 rdf:type obs:HighAvgSpeed }}  \n" +
                "WHERE\n" +
                "  { \n" +
                "   GRAPH <"+ruri+"> {" +
                "   ?obs1 rdf:type obs:Observation .\n" +
                "    ?obs1 obs:hasVelocity ?v3 .\n" +
                "    ?obs1 obs:hasSender ?snd1 .\n" +
                "    ?obs1 obs:hasID ?ID3 .\n" +
                "    ?obs1 obs:hasDate ?date3.\n" +
                "    FILTER(?ID3 = (?ID2 + 1))\n" +
                "    FILTER ( ?v3 > 80 )\n" +
                "    { SELECT ?v2 ?v1 ?date1 ?ID2 ?snd1\n" +
                "      WHERE\n" +
                "        { ?obs1 obs:hasVelocity ?v2 .\n" +
                "          ?obs1 obs:hasSender ?snd1 .\n" +
                "          ?obs1 obs:hasID ?ID2.            \n" +
                "\t  FILTER(?ID2 = (?ID1 + 1))\n" +
                "          FILTER ( ?v2 > 80 ) \n" +
                "\t    { SELECT ?v1 ?date1 ?ID1 ?snd1\n" +
                "\t      WHERE\n" +
                "\t\t{ ?obs1 obs:hasVelocity ?v1 .\n" +
                "\t\t  ?obs1 obs:hasSender ?snd1 .\n" +
                "\t\t  ?obs1 obs:hasID ?ID1.  \n" +
                "\t\t  ?obs1 obs:hasDate ?date1. \n" +
                "                  FILTER ( ?v1 > 80 ) \n" +
                "\t\t\t\n" +
                "\t\t}\n" +
                "\t    }        \n" +
                "        }\n" +
                "    }  \n" +
                "      \n" +
                "    BIND(?v1 + ?v2 + ?v3 AS ?vs)  \n" +
                "    FILTER(?vs > 240)      \n" +
                "\n" +
                "  }}";
        return query;
    }

    public static String deleteTempObservations(String resourceUri) {

        String qry =  "PREFIX  rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX  obs:  <"+resourceUri+">\n" +
                "PREFIX  xsd:  <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX  owl:  <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX  rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "\n" +
                "DELETE {?s ?p ?o}\n" +
                "WHERE {\n" +
                "    ?s rdf:type obs:Observation. ?s ?p ?o    \n" +
                "}";

        return qry;
    }

}
