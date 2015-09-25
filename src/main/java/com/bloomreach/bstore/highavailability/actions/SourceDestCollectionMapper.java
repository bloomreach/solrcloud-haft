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
package com.bloomreach.bstore.highavailability.actions;

import com.bloomreach.bstore.highavailability.exceptions.CollectionPatternException;
import com.bloomreach.bstore.highavailability.zookeeper.ZkClusterData;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Source to Destination Collection Mapper. When you want to clone a collection from source to destination,
 * you might want to use a different collection name. Example: nitin_collection at source might need to be copied to
 * nitin_collection2 in the destination. In this case we should map _collection to _collection2. During the time of cloning,
 * we apply this pattern to identify the destination collection name and then clone the index.
 *
 * If the mapping is not set, we use the same collection name as the source.
 * @author nitin
 * @since 02/10/2014
 */
public class SourceDestCollectionMapper {
  protected static final Logger logger = Logger.getLogger(SolrFaultTolerantAction.class);

  private List<String[]> collectionRules;
  private String defaultCollectionAppend;
  private List<String[]> configRules;
  private String defaultConfig;
  private ZkClusterData sourceZkClusterData;

  public SourceDestCollectionMapper(String collectionNamePatterns, String configNamePatterns, ZkClusterData sourceZkClusterData) throws CollectionPatternException {
    collectionRules = new ArrayList<String[]>();
    configRules = new ArrayList<String[]>();
    this.sourceZkClusterData = sourceZkClusterData;

    if (!StringUtils.isBlank(collectionNamePatterns)) {
      String[] patterns = collectionNamePatterns.split(",");
      for (String pattern : patterns) {
        String[] rule = pattern.split(":");
        if (rule.length != 2) {
          throw new CollectionPatternException("Invalid match replace pattern " + collectionNamePatterns + " Should be <src>:<dest>");
        }
        if (rule[0].equalsIgnoreCase("default")) {
          defaultCollectionAppend = rule[1];
          logger.debug("Added collectionRule: [Default- No match append] --> " + defaultCollectionAppend);
          continue;
        }
        collectionRules.add(rule);
        logger.debug("Added collectionRule: " + rule[0] + " --> " + rule[1]);
      }
    }
    if (defaultCollectionAppend == null) {
      logger.error("No default Collection Mapping.. Using names as is..");
    }
    if (!StringUtils.isBlank(configNamePatterns)) {
      String[] patterns = configNamePatterns.split(",");
      for (String pattern : patterns) {
        String[] rule = pattern.split(":");
        if (rule.length != 2) {
          throw new CollectionPatternException("Bad config name pattern: " + pattern + " must be of type <match>:<config name>");
        }
        if (rule[0].equalsIgnoreCase("default")) {
          defaultConfig = rule[1];
          logger.debug("Added ConfigRule: [Default- No match append] --> " + defaultConfig);
          continue;
        }
        configRules.add(rule);
        logger.debug("Added ConfigRule: " + rule[0] + " --> " + rule[1]);
      }
    }

    if (defaultConfig == null) {
      logger.error("No default Config Mapping.. Using names as is..");
    }
  }


  /**
   * Given a source collection name, find the destination collection name by the application of the
   * collection name rules.
   * @param srcCollection
   * @return String source Collection Name
   */
  public String dest(String srcCollection) {
    String destCollection = mapCollectionName(srcCollection, true);
    logger.info("Mapped Src:" + srcCollection + " --> Dst:" + destCollection);
    return destCollection;
  }

  /**
   * Given a destination collection name, find the source collection name by reverse application of the
   * collection name rules.
   * @param destCollection
   * @return String source Collection Name
   */
  public String src(String destCollection) {
    String srcCollection = mapCollectionName(destCollection, false);
    logger.info("Mapped Dst:" + destCollection + " --> Src:" + srcCollection);
    return srcCollection;
  }

  /**
   * Given a collection name and a flag (reverse of forward mapping), return the new
   * collection name based on the mapping
   * @param collectionName
   * @param dest
   * @return String new Collection Name
   */
  private String mapCollectionName(String collectionName, boolean dest) {
    //If there are no collection Rules, use the same collection Name as source
    if ((collectionRules.size() == 0) && (StringUtils.isBlank(defaultCollectionAppend))) {
      return collectionName;
    }
    //With the given rules, if the collection name matches the rule, then apply the rule and identify
    //the new collection name
    for (String[] rule : collectionRules) {
      String match = (dest) ? rule[0] : rule[1];
      String replace = (dest) ? rule[1] : rule[0];
      if (collectionName.contains(match)) {
        return collectionName.replace(match, replace);
      }
    }
    //Forward mapping
    if (dest) {
      return collectionName + defaultCollectionAppend;
    } else {
      //Reverse mapping. (Dest -> Source)
      return collectionName.replace(defaultCollectionAppend, "");
    }
  }

  /**
   * Given a collection name find out the zookeeper config name based on the mapping.
   * @param collectionName
   * @return
   * @throws CollectionPatternException
   */
  public String config(String collectionName) throws CollectionPatternException {
    //If there are no rules, use whatever config Name the source Cluster has
    if ((configRules.size() == 0) && (StringUtils.isBlank(defaultConfig))) {
      String configName = "";
      if (sourceZkClusterData != null) {
        configName = sourceZkClusterData.getCollectionToConfigMapping().get(collectionName);
      }
      if (StringUtils.isBlank(configName)) {
        throw new CollectionPatternException("Collections " + collectionName + " does not exist in the source Cluster.. ");
      }
      return configName;
    }
    for (String[] rule : configRules) {
      if (collectionName.contains(rule[0])) {
        logger.debug("MappedConfig Collection: " + collectionName + " --> Config:" + rule[1]);
        return rule[1];
      }
    }
    logger.debug("MappedConfig Collection: " + collectionName + " --> DefaultConfig:" + defaultConfig);
    return defaultConfig;
  }
}
