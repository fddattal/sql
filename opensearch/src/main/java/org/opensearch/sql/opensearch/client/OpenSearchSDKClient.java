package org.opensearch.sql.opensearch.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.IndexSettings;
import org.opensearch.sdk.Client;
import org.opensearch.sdk.model.ClearScrollRequest;
import org.opensearch.sdk.model.Data;
import org.opensearch.sdk.model.GetIndexMappingsRequest;
import org.opensearch.sdk.model.GetIndexMappingsResponse;
import org.opensearch.sdk.model.GetIndexSettingRequest;
import org.opensearch.sdk.model.GetIndexSettingResponse;
import org.opensearch.sdk.model.IndexExpression;
import org.opensearch.sdk.model.IndexExpressions;
import org.opensearch.sdk.model.IndexName;
import org.opensearch.sdk.model.MatchAllQuery;
import org.opensearch.sdk.model.ResolveIndicesAndAliasesRequest;
import org.opensearch.sdk.model.ResolveIndicesAndAliasesResponse;
import org.opensearch.sdk.model.ScrollCursor;
import org.opensearch.sdk.model.SettingKey;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

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

    private List<IndexName> resolveIndicesAndAliases(String... indexExpression) {

        var request = ResolveIndicesAndAliasesRequest.builder()
                .indexExpressions(
                        IndexExpressions.builder()
                                .expressions(
                                        Arrays.stream(indexExpression)
                                                .map(ie ->
                                                        IndexExpression.builder()
                                                                .value(ie)
                                                                .build()
                                                )
                                                .collect(Collectors.toList())
                                )
                                .build()
                )
                // TODO
                .indicesOptions(null)
                .build();

        CompletionStage<ResolveIndicesAndAliasesResponse> resolveFuture = client.resolveIndicesAndAliases(request);

        ResolveIndicesAndAliasesResponse resolveResponse = Futures.getUnchecked(resolveFuture.toCompletableFuture());

        return resolveResponse.getIndexNames()
                .getNames();
    }

    @Override
    public Map<String, IndexMapping> getIndexMappings(String... indexExpression) {

        Map<String, IndexMapping> indexMappings = new HashMap<>();

        for (IndexName indexName : resolveIndicesAndAliases(indexExpression)) {

            CompletionStage<GetIndexMappingsResponse> getMappingFuture = client.getIndexMappings(
                    GetIndexMappingsRequest.builder()
                            .indexName(indexName)
                            .build()
            );

            GetIndexMappingsResponse getMappingResponse = Futures.getUnchecked(getMappingFuture.toCompletableFuture());

            IndexMapping indexMapping = new IndexMapping(getMappingResponse.getMappingMetadata());

            indexMappings.put(indexName.getValue(), indexMapping);
        }

        return indexMappings;
    }

    @Override
    public Map<String, Integer> getIndexMaxResultWindows(String... indexExpression) {

        Map<String, Integer> result = new HashMap<>();

        for (IndexName indexName : resolveIndicesAndAliases(indexExpression)) {

            CompletionStage<GetIndexSettingResponse> future = client.getIndexSetting(
                    GetIndexSettingRequest.builder()
                            .indexName(indexName)
                            .settingKey(
                                    SettingKey.builder()
                                    .value(IndexSettings.MAX_RESULT_WINDOW_SETTING.getKey())
                                    .build()
                            )
                            .build()
            );

            GetIndexSettingResponse response = Futures.getUnchecked(future.toCompletableFuture());

            String settingValueAsString = response.getSettingValue().getValue();
            Integer settingValue = Integer.valueOf(settingValueAsString);

            result.put(indexName.getValue(), settingValue);
        }

        return result;
    }

    @Override
    public OpenSearchResponse search(OpenSearchRequest request) {
        return request.search(this::doSearch, this::doScroll);
    }

    private SearchResponse doSearch(SearchRequest searchRequest) {

        // TODO: expand the scope to more than just this class

        CompletionStage<org.opensearch.sdk.model.SearchResponse> future = client.search(
                org.opensearch.sdk.model.SearchRequest.builder()
                        .from(searchRequest.source().from())
                        .size(searchRequest.source().size())
                        // TODO query transformation
                        .query(MatchAllQuery.builder().build())
                        .build()
        );

        org.opensearch.sdk.model.SearchResponse response = Futures.getUnchecked(future.toCompletableFuture());

        // TODO: response transformation

        SearchHit[] hits = new SearchHit[response.getHits().getSearchHits().size()];
        for (int i = 0; i < hits.length; i++) {
            org.opensearch.sdk.model.SearchHit sdkHit = response.getHits().getSearchHits().get(i);

            ObjectMapper objectMapper = new ObjectMapper();
            byte[] bytes = new byte[0];
            try {
                bytes = objectMapper.writeValueAsBytes(Data.unwrap(sdkHit.getSource().getValue()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to convert source to byte array", e);
            }
            BytesArray bytesArray = new BytesArray(bytes);

            hits[i] = SearchHit.createFromMap(Map.of(
                    "_id", sdkHit.getDocumentId().getValue(),
                    "_index", sdkHit.getIndexName().getValue(),
                    "_score", sdkHit.getScore().getValue(),
                    "_source", bytesArray
            ));
        }

        return new SearchResponse(
                new SearchResponseSections(
                        new SearchHits(
                                hits,
                                null,
                                1.0f
                        ),
                        null,
                        null,
                        false,
                        false,
                        null,
                        0
                ),
                null,
                0,
                0,
                0,
                0L,
                new ShardSearchFailure[0],
                null
        );
    }

    private SearchResponse doScroll(SearchScrollRequest scrollRequest) {
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
        request.clean(scrollId -> {
            var future = client.clearScroll(
                    ClearScrollRequest.builder()
                            .scrollCursor(
                                    ScrollCursor.builder()
                                            .value(scrollId)
                                            .build()
                            )
                            .build()
            );

            // wait for it to complete
            Futures.getUnchecked(future.toCompletableFuture());
        });
    }

    @Override
    public void schedule(Runnable task) {
        task.run();
    }

    @Override
    public NodeClient getNodeClient() {
        throw new UnsupportedOperationException();
    }
}
