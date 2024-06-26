/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.planner;

import org.junit.Test;
import org.opensearch.sql.legacy.query.planner.core.QueryPlanner;

/** Query planner explanation unit test */
public class QueryPlannerExplainTest extends QueryPlannerTest {

  @Test
  public void explainInJson() {
    QueryPlanner planner =
        plan(
            "SELECT d.name, e.lastname FROM employee e "
                + "  JOIN department d ON d.id = e.departmentId "
                + "    WHERE d.region = 'US' AND e.age > 30");
    planner.explain();
  }

  @Test
  public void explainInJsonWithComplicatedOn() {
    QueryPlanner planner =
        plan(
            "SELECT d.name, e.lastname FROM employee e "
                + "  JOIN department d ON d.id = e.departmentId AND d.location = e.region "
                + "    WHERE d.region = 'US' AND e.age > 30");
    planner.explain();
  }

  @Test
  public void explainInJsonWithDuplicateColumnsPushedDown() {
    QueryPlanner planner =
        plan(
            "SELECT d.id, e.departmentId FROM employee e "
                + "  JOIN department d ON d.id = e.departmentId AND d.location = e.region "
                + "    WHERE d.region = 'US' AND e.age > 30");
    planner.explain();
  }
}
