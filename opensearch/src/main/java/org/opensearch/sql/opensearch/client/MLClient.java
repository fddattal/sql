package org.opensearch.sql.opensearch.client;

import org.opensearch.client.support.AbstractClient;
import org.opensearch.ml.client.MachineLearningNodeClient;

public class MLClient {
  private static MachineLearningNodeClient INSTANCE;

  private MLClient() {}

  /**
   * get machine learning client.
   *
   * @param nodeClient node client
   * @return machine learning client
   */
  public static MachineLearningNodeClient getMLClient(AbstractClient nodeClient) {
    if (INSTANCE == null) {
      INSTANCE = new MachineLearningNodeClient(nodeClient);
    }
    return INSTANCE;
  }
}
