/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_CSV_SANITIZE;
import static org.opensearch.sql.protocol.response.format.FlatResponseFormatter.CONTENT_TYPE;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class CsvFormatIT extends SQLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK_CSV_SANITIZE);
  }

  @Test
  public void sanitizeTest() {
    String result =
        executeQuery(
            String.format(
                Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE),
            "csv");
    assertRowsEqual(
        StringUtils.format(
            "firstname,lastname%n"
                + "'+Amber JOHnny,Duke Willmington+%n"
                + "'-Hattie,Bond-%n"
                + "'=Nanette,Bates=%n"
                + "'@Dale,Adams@%n"
                + "\",Elinor\",\"Ratliff,,,\"%n"),
        result);
  }

  @Test
  public void escapeSanitizeTest() {
    String result =
        executeQuery(
            String.format(
                Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE),
            "csv&sanitize=false");
    assertRowsEqual(
        StringUtils.format(
            "firstname,lastname%n"
                + "+Amber JOHnny,Duke Willmington+%n"
                + "-Hattie,Bond-%n"
                + "=Nanette,Bates=%n"
                + "@Dale,Adams@%n"
                + "\",Elinor\",\"Ratliff,,,\"%n"),
        result);
  }

  @Test
  public void contentHeaderTest() throws IOException {
    String query =
        String.format(
            Locale.ROOT, "SELECT firstname, lastname FROM %s", TEST_INDEX_BANK_CSV_SANITIZE);
    String requestBody = makeRequest(query);

    Request sqlRequest = new Request("POST", "/_plugins/_sql?format=csv");
    sqlRequest.setJsonEntity(requestBody);

    Response response = client().performRequest(sqlRequest);

    assertEquals(response.getEntity().getContentType().getValue(), CONTENT_TYPE);
  }

  private void assertRowsEqual(String expected, String actual) {
    if (expected.equals(actual)) {
      return;
    }

    List<String> expectedLines = List.of(expected.split("\n"));
    List<String> actualLines = List.of(actual.split("\n"));

    if (expectedLines.size() != actualLines.size()) {
      Assert.fail("Line count is different " + expected + " " + actual);
    }

    if (!expectedLines.get(0).equals(actualLines.get(0))) {
      Assert.fail("Header is different " + expected + " " + actual);
    }

    Set<String> expectedItems = new HashSet<>(expectedLines.subList(1, expectedLines.size()));
    Set<String> actualItems = new HashSet<>(actualLines.subList(1, actualLines.size()));

    assertEquals(expectedItems, actualItems);
  }
}
