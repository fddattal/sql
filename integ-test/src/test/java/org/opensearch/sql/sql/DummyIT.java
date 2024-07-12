package org.opensearch.sql.sql;

import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.OpenSearchSQLRestTestCase;

public class DummyIT extends OpenSearchSQLRestTestCase {
    @Test
    public void dummyTest() {
        Assert.assertEquals(true, true);
    }
}
