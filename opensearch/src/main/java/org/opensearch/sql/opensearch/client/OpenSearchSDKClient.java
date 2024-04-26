package org.opensearch.sql.opensearch.client;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.node.NodeClient;
import org.opensearch.sdk.Client;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class OpenSearchSDKClient implements OpenSearchClient {

    private final Client client;

    @Override
    public boolean exists(String indexName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createIndex(String indexName, Map<String, Object> mappings) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OpenSearchResponse search(OpenSearchRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> indices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> meta() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup(OpenSearchRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void schedule(Runnable task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NodeClient getNodeClient() {
        throw new UnsupportedOperationException();
    }
}
