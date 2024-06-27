/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import org.junit.experimental.categories.Category;
import org.opensearch.category.CloudNative;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class SimpleIT extends SQLIntegTestCase {
  @Category(CloudNative.class)
  public void simpleDummyTest() {
    System.out.println("Hello, World!");
  }
}
