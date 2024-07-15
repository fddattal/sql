package org.opensearch.sql.sql;

import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.client.RestClient;
import org.opensearch.sql.legacy.OpenSearchSQLRestTestCase;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class DummyIT extends OpenSearchSQLRestTestCase {

    private RestClient mockClient;

    @SneakyThrows
    @Before
    public void initClient2() {
        System.out.println("initClient2");

        mockClient = mockRestClientForPoc();

        set(OpenSearchSQLRestTestCase.class.getDeclaredField("remoteClient"), mockClient);
        set(OpenSearchSQLRestTestCase.class.getDeclaredField("remoteAdminClient"), mockClient);
        set(OpenSearchRestTestCase.class.getDeclaredField("client"), mockClient);
        set(OpenSearchRestTestCase.class.getDeclaredField("adminClient"), mockClient);
    }

    @SneakyThrows
    private static void set(Field field, Object value) {
        field.setAccessible(true);
        field.set(null, value);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // not necessary for client injection, just used to avoid failure due to mock client
        return true;
    }

    @Test
    public void clientTest() {
        Assert.assertSame(mockClient, client());
        Assert.assertSame(mockClient, remoteClient());
        Assert.assertSame(mockClient, adminClient());
        Assert.assertSame(mockClient, remoteAdminClient());
    }

    @SneakyThrows
    private static RestClient mockRestClientForPoc() {

        List<RestClient> initialClients = new ArrayList<>();

        add(initialClients, client());
        add(initialClients, remoteClient());
        add(initialClients, adminClient());
        add(initialClients, remoteAdminClient());

        RestClient mockClient = Mockito.mock(RestClient.class);

        Mockito.doAnswer(args -> {

            for (RestClient restClient : initialClients) {
                restClient.close();
            }

            return null;
        }).when(mockClient).close();

        return mockClient;
    }

    private static void add(List<RestClient> clients, RestClient restClient) {
        if (restClient == null) {
            return;
        }
        clients.add(restClient);
    }
}
