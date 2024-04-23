/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.opensearch.client.*;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import javax.net.ssl.SSLContext;

/**
 * OpenSearch SQL integration test base class to support both security disabled and enabled
 * OpenSearch cluster. Allows interaction with multiple external test clusters using OpenSearch's
 * {@link RestClient}.
 */
public abstract class OpenSearchSQLRestTestCase extends OpenSearchRestTestCase {

  private static final Logger LOG = LogManager.getLogger();
  public static final String MATCH_ALL_REMOTE_CLUSTER = "*";
  // Requires to insert cluster name and cluster transport address (host:port)
  public static final String REMOTE_CLUSTER_SETTING =
      "{"
          + "\"persistent\": {"
          + "  \"cluster\": {"
          + "    \"remote\": {"
          + "      \"%s\": {"
          + "        \"seeds\": ["
          + "          \"%s\""
          + "        ]"
          + "      }"
          + "    }"
          + "  }"
          + "}"
          + "}";

  private static RestClient remoteClient;

  /**
   * A client for the running remote OpenSearch cluster configured to take test administrative
   * actions like remove all indexes after the test completes
   */
  private static RestClient remoteAdminClient;

  private static final Object LOCK = new Object();

  public OpenSearchSQLRestTestCase() {
    synchronized (LOCK) {
      if (remoteAdminClient == null && remoteClient == null) {
        try {
          remoteClient = remoteAdminClient = initClient("docker-cluster");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  protected static RestClient client() {
    return Objects.requireNonNull(remoteClient);
  }

  protected static RestClient adminClient() {
    return Objects.requireNonNull(remoteAdminClient);
  }

  protected boolean isHttps() {
    boolean isHttps =
        Optional.ofNullable(System.getProperty("https"))
            .map("true"::equalsIgnoreCase)
            .orElse(false);
    if (isHttps) {
      // currently only external cluster is supported for security enabled testing
      if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
        throw new RuntimeException(
            "external cluster url should be provided for security enabled testing");
      }
    }

    return isHttps;
  }

  protected String getProtocol() {
    return isHttps() ? "https" : "http";
  }

  /** Get the client to remote cluster used for ordinary api calls while writing a test. */
  protected static RestClient remoteClient() {
    return remoteClient;
  }

  /**
   * Get the client to remote cluster used for test administrative actions. Do not use this while
   * writing a test. Only use it for cleaning up after tests.
   */
  protected static RestClient remoteAdminClient() {
    return remoteAdminClient;
  }

  protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
    JunoRestClientBuilder builder = JunoRestClient.junoBuilder(hosts);
    if (isHttps()) {
      configureHttpsClient(builder, settings);
    } else {
      configureClient(builder, settings);
    }

    builder.setStrictDeprecationMode(false);
    return builder.build();
  }

  // Modified from initClient in OpenSearchRestTestCase
  public void initRemoteClient(String clusterName) throws IOException {
    remoteClient = remoteAdminClient = initClient(clusterName);
  }

  /** Configure http client for the given <b>cluster</b>. */
  public RestClient initClient(String clusterName) throws IOException {
    String[] stringUrls = getTestRestCluster(clusterName).split(",");
    List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
    for (String stringUrl : stringUrls) {
      int portSeparator = stringUrl.lastIndexOf(':');
      if (portSeparator < 0) {
        throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
      }
      String host = stringUrl.substring(0, portSeparator);
      int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
      hosts.add(buildHttpHost(host, port));
    }
    return buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
  }

  /** Get a comma delimited list of [host:port] to which to send REST requests. */
  protected String getTestRestCluster(String clusterName) {
//    String cluster = System.getProperty("tests.rest." + clusterName + ".http_hosts");
//    if (cluster == null) {
//      throw new RuntimeException(
//          "Must specify [tests.rest."
//              + clusterName
//              + ".http_hosts] system property with a comma delimited list of [host:port] "
//              + "to which to send REST requests");
//    }
//    return cluster;
    return "localhost:9200";
  }

  /** Get a comma delimited list of [host:port] for connections between clusters. */
  protected String getTestTransportCluster(String clusterName) {
    String cluster = System.getProperty("tests.rest." + clusterName + ".transport_hosts");
    if (cluster == null) {
      throw new RuntimeException(
          "Must specify [tests.rest."
              + clusterName
              + ".transport_hosts] system property with a comma delimited list of [host:port] "
              + "for connections between clusters");
    }
    return cluster;
  }

  @AfterClass
  public static void closeRemoteClients() throws IOException {
    try {
      IOUtils.close(remoteClient, remoteAdminClient);
    } finally {
      remoteClient = null;
      remoteAdminClient = null;
    }
  }

  protected static void wipeAllOpenSearchIndices() throws IOException {
    // TODO - temporarily disable to view metadata
//    wipeAllOpenSearchIndices(client());
//    if (remoteClient() != null) {
//      wipeAllOpenSearchIndices(remoteClient());
//    }
  }

  protected static void wipeAllOpenSearchIndices(RestClient client) throws IOException {
    // include all the indices, included hidden indices.
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html#cat-indices-api-query-params
    Response response =
//        client.performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
        client.performRequest(new Request("GET", "/_cat/indices?format=json"));
    JSONArray jsonArray = new JSONArray(EntityUtils.toString(response.getEntity(), "UTF-8"));
    for (Object object : jsonArray) {
      JSONObject jsonObject = (JSONObject) object;
      String indexName = jsonObject.getString("index");
      try {
        // System index, mostly named .opensearch-xxx or .opendistro-xxx, are not allowed to delete
        if (!indexName.startsWith(".opensearch") && !indexName.startsWith(".opendistro") && !indexName.startsWith("target_index")) {
          System.out.println("Deleting index " + indexName);
          client.performRequest(new Request("DELETE", "/" + indexName));
        }
      } catch (Exception e) {
        // TODO: Ignore index delete error for now. Remove this if strict check on system index
        // added above.
        LOG.warn("Failed to delete index: " + indexName, e);
      }
    }
  }

  /**
   * Configure authentication and pass <b>builder</b> to superclass to configure other stuff.<br>
   * By default, auth is configure when <b>https</b> is set only.
   */
  protected static void configureClient(JunoRestClientBuilder builder, Settings settings)
      throws IOException {
    String userName = System.getProperty("user");
    String password = System.getProperty("password");
    if (userName != null && password != null) {
      builder.setHttpClientConfigCallback(
          httpClientBuilder -> {
            JunoRestClient.configureHttpRequestHeaders(httpClientBuilder);
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                new AuthScope(null, -1), new UsernamePasswordCredentials(userName, password));
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          });
    }
    openSearchRestTestCaseConfigureClient(builder, settings);
  }

  protected static void openSearchRestTestCaseConfigureClient(JunoRestClientBuilder builder, Settings settings) throws IOException {
    String keystorePath = settings.get(TRUSTSTORE_PATH);
    if (keystorePath != null) {
      final String keystorePass = settings.get(TRUSTSTORE_PASSWORD);
      if (keystorePass == null) {
        throw new IllegalStateException(TRUSTSTORE_PATH + " is provided but not " + TRUSTSTORE_PASSWORD);
      }
      Path path = PathUtils.get(keystorePath);
      if (!Files.exists(path)) {
        throw new IllegalStateException(TRUSTSTORE_PATH + " is set but points to a non-existing file");
      }
      try {
        final String keyStoreType = keystorePath.endsWith(".p12") ? "PKCS12" : "jks";
        KeyStore keyStore = KeyStore.getInstance(keyStoreType);
        try (InputStream is = Files.newInputStream(path)) {
          keyStore.load(is, keystorePass.toCharArray());
        }
        SSLContext sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
        SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslcontext);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
          JunoRestClient.configureHttpRequestHeaders(httpClientBuilder);
          return httpClientBuilder.setSSLStrategy(sessionStrategy);
        });
      } catch (KeyStoreException | NoSuchAlgorithmException | KeyManagementException | CertificateException e) {
        throw new RuntimeException("Error setting up ssl", e);
      }
    }
    Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
    Header[] defaultHeaders = new Header[headers.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
    }
    builder.setDefaultHeaders(defaultHeaders);
    final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
    final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
    );
    builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
    if (settings.hasValue(CLIENT_PATH_PREFIX)) {
      builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
    }
  }

  protected static void configureHttpsClient(JunoRestClientBuilder builder, Settings settings)
      throws IOException {
    Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
    Header[] defaultHeaders = new Header[headers.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
    }
    builder.setDefaultHeaders(defaultHeaders);
    builder.setHttpClientConfigCallback(
        httpClientBuilder -> {
          JunoRestClient.configureHttpRequestHeaders(httpClientBuilder);
          String userName =
              Optional.ofNullable(System.getProperty("user"))
                  .orElseThrow(() -> new RuntimeException("user name is missing"));
          String password =
              Optional.ofNullable(System.getProperty("password"))
                  .orElseThrow(() -> new RuntimeException("password is missing"));
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(
              AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
          try {
            return httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider)
                // disable the certificate since our testing cluster just uses the default security
                // configuration
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .setSSLContext(
                    SSLContextBuilder.create()
                        .loadTrustMaterial(null, (chains, authType) -> true)
                        .build());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
    final TimeValue socketTimeout =
        TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
    builder.setRequestConfigCallback(
        conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
    if (settings.hasValue(CLIENT_PATH_PREFIX)) {
      builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
    }
  }

  /**
   * Initialize rest client to remote cluster, and create a connection to it from the coordinating
   * cluster.
   */
  public void configureMultiClusters(String remote) throws IOException {
    initRemoteClient(remote);

    Request connectionRequest = new Request("PUT", "_cluster/settings");
    String connectionSetting =
        String.format(
            REMOTE_CLUSTER_SETTING, remote, getTestTransportCluster(remote).split(",")[0]);
    connectionRequest.setJsonEntity(connectionSetting);
    adminClient().performRequest(connectionRequest);
  }
}
