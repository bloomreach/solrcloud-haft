/**
 * Copyright 2014-2015 BloomReach, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bloomreach.bstore.highavailability.utils;

import com.bloomreach.bstore.highavailability.models.ReplicationDiagnostics;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Utilities for interacting with solrcloud collections
 * @author nitin
 * @since 2/19/14.
 */
public class SolrInteractionUtils {
  private static final Logger logger = Logger.getLogger(SolrInteractionUtils.class);
  private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) " +
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36";
  private static final int DEFAULT_TIMEOUT = 300000;
  public static final int CONNECT_TIMEOUT = 15000;
  public static final int DEFAULT_SLEEP_TIME = 2000;
  public static final int DEFAULT_SOLR_PORT = 8983;


  /**
   * Create a core on a host given the shard, collection and coreName
   *
   * @param host
   * @param shard
   * @param coreName
   * @param collection
   * @throws java.io.IOException
   */
  public static void createCore(String host, String coreName, String collection, String shard) throws IOException {
    String createCoreCTemplate = "http://%s:%s/solr/admin/cores?action=CREATE&name=%s&collection=%s&shard=%s";
    String createCoreCommand = String.format(createCoreCTemplate, host, DEFAULT_SOLR_PORT, coreName, collection, shard);
    String result = executeSolrCommand(createCoreCommand);
    logger.info(result);
  }

  /**
   * Create an Alias for a given collection
   *
   * @param host
   * @param alias
   * @param collection
   * @throws java.io.IOException
   */
  public static void createAlias(String host, String alias, String collection) throws IOException {
    String createAliasTemplate = "http://%s:%s/solr/admin/collections?action=CREATEALIAS&name=%s&collections=%s";
    String createAliasCommand = String.format(createAliasTemplate, host, DEFAULT_SOLR_PORT, alias, collection);
    String result = executeSolrCommand(createAliasCommand);
    logger.info(result);
  }


  /**
   * Given a collection Name, it deletes it
   *
   * @param collection
   * @param host
   * @throws Exception
   */
  public static void deleteCollection(String host, String collection) throws Exception {
    String deleteCollectionTemplate = "http://%s:%s/solr/admin/collections?action=DELETE&name=%s";
    String deleteCollectionCommand = String.format(deleteCollectionTemplate, host, DEFAULT_SOLR_PORT, collection);
    executeSolrCommand(deleteCollectionCommand);
  }

  /**
   * Create a solr collection
   *
   * @param solrHosts
   * @param collectionName
   * @param configName
   * @param numShards
   * @param replicationFactor
   * @param numShardsPerNode
   * @throws Exception
   */
  public static void createCollection(Set<String> solrHosts, String collectionName, String configName, int numShards,
                                      int replicationFactor, int numShardsPerNode) throws Exception {
    createCollection(solrHosts, collectionName, configName, numShards, replicationFactor, numShardsPerNode, null);
  }

  /**
   * Create a solr collection with extra solrcore.properties path
   * if the solrcoreHttpPath is given then we will create the collection use a customized parameter
   * collection.dynamicProperties to create the collection with that file
   *
   * @param solrHosts
   * @param collectionName
   * @param configName
   * @param numShards
   * @param replicationFactor
   * @param numShardsPerNode
   * @param solrcoreHttpPath
   * @throws Exception
   */
  public static void createCollection(Set<String> solrHosts, String collectionName, String configName, int numShards,
                                      int replicationFactor, int numShardsPerNode, String solrcoreHttpPath) throws Exception {
    logger.info("createCollection this is the collection " + collectionName + "  and this is the replicationFactor: "
            + replicationFactor);
    String command = String
            .format(
                    "http://%s:%s/solr/admin/collections?wt=json&action=CREATE&name=%s&numShards=%d&replicationFactor=%d&collection.configName=%s&maxShardsPerNode=%d",
                    solrHosts.toArray()[0], DEFAULT_SOLR_PORT, collectionName, numShards, replicationFactor, configName, numShardsPerNode);
    if (!StringUtils.isBlank(solrcoreHttpPath)) {
      command += "&collection.solrcoreproperties=" + solrcoreHttpPath;
    }
    logger.info("createCollection this is the collection " + collectionName + "  and this is the command: " + command);
    String result = SolrInteractionUtils.executeSolrCommand(command);
    logger.info(result);
  }


  /**
   * Query Collection and return number of documents
   *
   * @throws Exception
   */
  public static Integer fetchCollectionSize(String collection, String host) throws Exception {
    String command = String.format(
            "http://%s:%s/solr/%s/select?q=*:*&wt=json", host, DEFAULT_SOLR_PORT, collection);
    String result = SolrInteractionUtils.executeSolrCommand(command);
    logger.info("Collection Size command is " + command);
    ObjectMapper mapper = new ObjectMapper();

    JsonNode node = mapper.readTree(result);
    int numDocs = Integer.parseInt(node.get("response").get("numFound").toString());
    return numDocs;
  }


  /**
   * Given a source core, destination core and hosts, replicate index from source to destination
   *
   * @param destHostName
   * @param destCore
   * @param sourceHost
   * @param sourceCore
   */
  public static void replicateIndex(String destHostName, String destCore, String sourceHost, String sourceCore) throws Exception {
    String fullReplication = "http://%s:%s/solr/%s/replication?command=fetchindex&masterUrl=http://%s:%s/solr/%s";
    String replicator = String.format(fullReplication, destHostName, DEFAULT_SOLR_PORT, destCore, sourceHost, DEFAULT_SOLR_PORT, sourceCore);
    String result = executeSolrCommand(replicator);
    logger.info(result);

  }

  /**
   * Execute a Solr Api call through Http
   *
   * @param command
   * @throws java.io.IOException
   */
  public static String executeSolrCommand(String command) throws IOException {
    logger.info(command);
    return executeSolrCommandWithTimeout(DEFAULT_TIMEOUT, command);
  }

  /**
   * Execute a Solr Api call through Http
   *
   * @param command
   * @throws java.io.IOException
   */
  public static String executeSolrCommandWithTimeout(int timeout, String command) throws IOException {
    logger.info(command);
    BufferedReader in = null;
    try {
      in = new BufferedReader(
              new InputStreamReader(executeSolrCommandAndGetInputStreamWithTimeout(timeout, command)));
      String inputLine;
      StringBuffer response = new StringBuffer();

      while ((inputLine = in.readLine()) != null) {
        response.append(inputLine);
      }
      in.close();

      return response.toString();
    } finally {
      if (in != null)
        in.close();
    }
  }

  /**
   * Execute Http Command with Default Timeout
   *
   * @param command
   * @return
   * @throws IOException
   */
  public static InputStream executeSolrCommandAndGetInputStream(String command) throws IOException {
    //logger.info("Command to Execute: " + command);
    return executeSolrCommandAndGetInputStreamWithTimeout(DEFAULT_TIMEOUT, command);
  }


  /**
   * Execute a Http Command with a given read Timeout
   *
   * @param timeout
   * @param command
   * @return {@link java.io.InputStream} of the obtained response
   * @throws IOException
   */
  public static InputStream executeSolrCommandAndGetInputStreamWithTimeout(int timeout, String command) throws IOException {
    //logger.info("Command to Execute: " + command);
    URL obj = new URL(command);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    con.setConnectTimeout(timeout); //set timeout to 30 seconds
    con.setReadTimeout(timeout); //set timeout to 30 seconds
    return con.getInputStream();
  }



  /**
   * Parse Status of given XPath Patterns
   *
   * @param result
   * @param xpathPatterns
   * @return {@link java.util.List<String>} of the xpath data.
   * @throws SAXException
   * @throws IOException
   * @throws ParserConfigurationException
   * @throws XPathExpressionException
   */
  public static List<String> parseStatusResponse(InputStream result, String[] xpathPatterns) throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(result);
    List<String> data = new ArrayList<String>();
    if (doc == null) {
      return data;
    }
    XPathFactory xPathfactory = XPathFactory.newInstance();
    XPath xpath = xPathfactory.newXPath();
    for (String pattern : xpathPatterns) {
      data.add(xpath.compile(pattern).evaluate(doc));
    }
    return data;
  }

  /**
   * Returns the replication status of a core on a given host
   *
   * @param host Solr Host to query
   * @param core name of the core to check replication
   * @return {@link com.bloomreach.bstore.highavailability.models.ReplicationDiagnostics} information about replication
   * @throws IOException
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws XPathExpressionException
   */
  public static ReplicationDiagnostics checkReplicationStatus(String host, String core)
          throws IOException, ParserConfigurationException, SAXException, XPathExpressionException {
    ReplicationDiagnostics diagnostics = new ReplicationDiagnostics();
    diagnostics.setEntity(core);
    String statusUrl = String.format("http://%s:%s/solr/%s/replication?command=details", host, SolrInteractionUtils.DEFAULT_SOLR_PORT, core);
    InputStream result = null;
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = null;
    try {
      result = SolrInteractionUtils.executeSolrCommandAndGetInputStream(statusUrl);
      doc = dBuilder.parse(result);
    } finally {
      if (result != null) {
        result.close();
      }
    }
    if (doc == null) {
      logger.info("Replicationstatus: " + host + " Not results");
      return diagnostics;
    }
    XPathFactory xPathfactory = XPathFactory.newInstance();
    XPath xpath = xPathfactory.newXPath();
    String curDate = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'currentDate']/text()").evaluate(doc);
    String isReplicating = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'isReplicating']/text()").evaluate(doc);
    //For some collections, when we query replication details, the replication hasn't started yet and the <slave> tag has not been generated yet.
    //That means the replication has not started yet.
    //Once the tag is generated, there will always be a field called currentDate, we use this tag to check if the replication started
    //Solr some times return something under the filed isReplicating, some times not and the replication normally already finished, so if
    //the currentDate field is populated and no info for isReplicating, that means the replication finished
    boolean replicating = !StringUtils.equalsIgnoreCase(isReplicating, "false");
    if (StringUtils.isBlank(curDate)) {
      replicating = true;
    } else {
      if (StringUtils.isBlank(isReplicating)) {
        replicating = false;
      }
    }
    String downloadSpeed = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'downloadSpeed']/text()").evaluate(doc);
    String totalPercent = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'totalPercent']/text()").evaluate(doc);
    String timeElapsed = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'timeElapsed']/text()").evaluate(doc);
    String timeRemaining = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'timeRemaining']/text()").evaluate(doc);
    String bytesDownloaded = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'bytesDownloaded']/text()").evaluate(doc);
    String bytesToDownload = xpath.compile("/response/lst[@name='details']/lst[@name = 'slave']/str[@name = 'bytesToDownload']/text()").evaluate(doc);
    diagnostics.setPercentageComplete(totalPercent);
    diagnostics.setTimeElapsed(timeElapsed);
    logger.info(core + ": " + totalPercent + "% Bytes:" + bytesDownloaded + "/" + bytesToDownload + " @ " + downloadSpeed
            + " Time:" + timeElapsed + "/" + timeRemaining);
    logger.info("This is the return value " + replicating + "  isReplicating:  " + isReplicating);

    if (!replicating) {
      logger.info("REPLICATION COMPLETE: " + host + " : " + core);
    }

    diagnostics.setReplicating(replicating);
    return diagnostics;
  }

}
