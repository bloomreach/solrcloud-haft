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


import com.bloomreach.bstore.highavailability.exceptions.PropertyLoadException;
import org.codehaus.plexus.util.ExceptionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Reads External Aws Configs.
 *
 * @author nitin
 * @since 6/25/15.
 */
public class AwsConfigReader {
  /* Stores AWS Credentials */
  private static Properties properties = new Properties();

  static {
    try {
      readProps("aws_credentials.properties");
    } catch (PropertyLoadException e) {
      throw  new RuntimeException(ExceptionUtils.getFullStackTrace(e));
    }
  }

  public static String fetchAccessKey() {
    return properties.getProperty("accessKey");
  }

  public static String fetchSecretyKey() {
    return properties.getProperty("secretKey");
  }

  /**
   * Read Properties from resources/aws_credentials.properties
   *
   * @param fileName
   * @return {@link java.util.Properties} of the aws credentials
   * @throws IOException
   */
  private static Properties readProps(String fileName) throws PropertyLoadException {
    InputStream stream = AwsConfigReader.class.getClassLoader().getResourceAsStream(fileName);
    try {
      properties.load(stream);
    } catch (IOException e) {
      throw new PropertyLoadException("Cannot find the Aws Properties under resources/");
    }
    return properties;
  }
}
