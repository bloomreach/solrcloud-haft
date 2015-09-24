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
package com.bloomreach.bstore.highavailability.models;

import cascading.util.Pair;
import com.bloomreach.bstore.highavailability.exceptions.SolrCoreLoadException;
import com.bloomreach.bstore.highavailability.utils.SolrInteractionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Model to Represent the SolrCore Information
 *
 * @author nitin,li
 * @since 3/27/14.
 */
public class SolrCore {
  protected static final Logger logger = Logger.getLogger(SolrCore.class);
  public boolean available;
  public int numDocs;
  public int maxDocs;
  public int deletedDocs;
  public long version;
  public int segmentCount;
  public String lastModified;
  public long sizeInBytes;
  public String name;
  public String host;
  public boolean current;


  /**
   * Represents a solr core
   * @param host
   * @param name
   */
  public SolrCore(String host, String name) {
    this.host = host;
    this.name = name;
    this.available = false;
    numDocs = 0;
    maxDocs = 0;
    deletedDocs = 0;
    version = 0;
    segmentCount = 0;
    lastModified = null;
    sizeInBytes = 0;
    this.current = false;
  }


  /**
   * Load the solr core status
   * @throws IOException
   */
  public void loadStatus() throws IOException, SolrCoreLoadException {
    String command = String.format("http://%s:%s/solr/admin/cores?action=STATUS&core=%s", host, SolrInteractionUtils.DEFAULT_SOLR_PORT, name);
    InputStream result = null;
    try {
      result = SolrInteractionUtils.executeSolrCommandAndGetInputStream(command);

      // TODO: change to json pasring
      List<String> data = SolrInteractionUtils
              .parseStatusResponse(
                      result,
                      new String[]{
                              String.format(
                                      "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/int[@name='numDocs']/text()",
                                      name),
                              String.format(
                                      "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/int[@name='maxDoc']/text()",
                                      name),
                              String
                                      .format(
                                              "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/int[@name='deletedDocs']/text()",
                                              name),
                              String.format(
                                      "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/long[@name='version']/text()",
                                      name),
                              String
                                      .format(
                                              "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/int[@name='segmentCount']/text()",
                                              name),
                              String
                                      .format(
                                              "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/date[@name='lastModified']/text()",
                                              name),
                              String
                                      .format(
                                              "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/long[@name='sizeInBytes']/text()",
                                              name),
                              String.format("/response/lst[@name='initFailures']/lst[@name='%s']/text()", name),
                              String.format(
                                      "/response/lst[@name='status']/lst[@name='%s']/lst[@name='index']/bool[@name='current']/text()",
                                      name)});

      boolean isCleanCore = StringUtils.isBlank(data.get(7));
      if (!isCleanCore) {
        String message = String.format("Core %s  has initialization exceptions..", name);
        logger.info(message);
        throw new SolrCoreLoadException(message);
      }

      numDocs = Integer.parseInt(data.get(0));

      if (!StringUtils.isBlank(data.get(1))) {
        maxDocs = Integer.parseInt(data.get(1));
      }

      deletedDocs = Integer.parseInt(data.get(2));
      version = Long.parseLong(data.get(3));

      if (!StringUtils.isBlank(data.get(4))) {
        segmentCount = Integer.parseInt(data.get(4));
      }
      if (!StringUtils.isBlank(data.get(5))) {
        lastModified = data.get(5);
      }

      if (!StringUtils.isBlank(data.get(6))) {
        sizeInBytes = Long.parseLong(data.get(6));
      }

      if (!StringUtils.isBlank(data.get(8))) {
        this.current = Boolean.parseBoolean(data.get(8));
      }
      this.available = true;
    } catch (Exception e) {
      throw new SolrCoreLoadException("Exception while trying to get core status for " + name);
    } finally {
      if (result != null) {
        result.close();
      }
    }
  }

  /**
   * Flattened representation of the Solr Core.
   * @return
   */
  public String toString() {
    if (available) {
      return host + "/" + name + " [numDocs=" + numDocs + " version=" + version + "]";
    } else {
      return host + "/" + name + " [Unavailable]";
    }
  }


  /**
   * Given another solrcore, do a data match between both of these cores.
   * If it fails, specify a reason. The data match is computed base on index version, core availability
   * and last modified time. We can add more based on num docs but that is for future work.
   * @param replica
   * @return {@link cascading.util.Pair}
   * @return
   */
  public Pair<Boolean, String> dataMatch(SolrCore replica) {
    Pair<Boolean, String> dataMatchToReasonPair = new Pair<Boolean, String>(true, "");
    boolean success = true;
    String reason = "";
    if (!this.available) {
      reason = "DataMatch-Failed: this core not available: " + this.toString();
      success = false;
    }
    if (!replica.available) {
      reason = "DataMatch-Failed: replica core not available: " + replica.toString();
      success = false;
    }

    // If the source version is greater than the replication version, it is a failed replication. Destination version
    // can be greater than the source (solr can decide to increment version of index after replication on the
    // destination node)
    if (this.version > replica.version) {
      reason = ("DataMatch-Failed: Version mismatch: this:" + this.version + " replica:" + replica.version);
      success = false;
    }

    // We also need to do a sanity check on the last update time
    if (StringUtils.isBlank(this.lastModified) || StringUtils.isBlank(replica.lastModified)) {
      logger
              .warn("Either replica or source last index update time is unkown, will not do the timestamp check, source: "
                      + this.host + " " + this.name + " replica: " + replica.host + " " + replica.name);
    } else {
      String solrTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
      try {
        DateFormat format = new SimpleDateFormat(solrTimeFormat);
        Date sourceDate = format.parse(this.lastModified);
        Date replicaDate = format.parse(replica.lastModified);
        long delta = Math.abs(sourceDate.getTime() - replicaDate.getTime());
        logger.info("this is the delta: " + delta + " souce: " + this.name + "replica: " + replica.name);
        // If the time diff greater than 60 minutes, we will mark it as unsuccessful since our
        // replication timeout is set at 60 minutes
        if (delta > 60 * 60 * 1000) {
          reason += "  last index update time between source and replica are different more than 60 minutes: source: "
                  + this.lastModified + " replica: " + replica.lastModified;
          success = false;
        }
      } catch (Exception e) {
        logger.warn("Can't parse the date of last index update date, skipping last update time check, source: "
                + this.host + " " + this.name + " " + this.lastModified + " replica: " + replica.host + " "
                + replica.name + replica.lastModified);
        logger.warn("ST: " + ExceptionUtils.getFullStackTrace(e));
      }
    }

    dataMatchToReasonPair.setLhs(success);
    dataMatchToReasonPair.setRhs(reason);

    return dataMatchToReasonPair;
  }
}
