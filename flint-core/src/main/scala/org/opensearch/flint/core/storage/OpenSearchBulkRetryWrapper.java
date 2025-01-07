/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedPredicate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.flint.core.http.FlintRetryOptions;
import org.opensearch.flint.core.metrics.MetricConstants;
import org.opensearch.flint.core.metrics.MetricsUtil;
import org.opensearch.rest.RestStatus;

/**
 * Wrapper class for OpenSearch bulk API with retry and rate limiting capability.
 * TODO: remove Retry from name (also rename variables)
 */
public class OpenSearchBulkRetryWrapper {

  private static final Logger LOG = Logger.getLogger(OpenSearchBulkRetryWrapper.class.getName());

  private final RetryPolicy<BulkResponse> retryPolicy;
  private final BulkRequestRateLimiter rateLimiter;

  public OpenSearchBulkRetryWrapper(FlintRetryOptions retryOptions, BulkRequestRateLimiter rateLimiter) {
    this.retryPolicy = retryOptions.getBulkRetryPolicy(bulkItemRetryableResultPredicate);
    this.rateLimiter = rateLimiter;
  }

  // TODO: need test case using bulk with rate limiter

  /**
   * Bulk request with retry and rate limiting. Delegate bulk request to the client, and retry the
   * request if the response contains retryable failure. It won't retry when bulk call thrown
   * exception. In addition, adjust rate limit based on the responses.
   * @param client used to call bulk API
   * @param bulkRequest requests passed to bulk method
   * @param options options passed to bulk method
   * @return Last result
   */
  public BulkResponse bulk(RestHighLevelClient client, BulkRequest bulkRequest, RequestOptions options) {
    rateLimiter.acquirePermit(bulkRequest.requests().size());
    return bulkWithPartialRetry(client, bulkRequest, options);
  }

  private BulkResponse bulkWithPartialRetry(RestHighLevelClient client, BulkRequest bulkRequest,
      RequestOptions options) {
    final AtomicInteger requestCount = new AtomicInteger(0);
    // TODO: notice for metric each retry attempt counts, but rate limit doesn't restrict retries
    // could appear weird in dashboards
    try {
      final AtomicReference<BulkRequest> nextRequest = new AtomicReference<>(bulkRequest);
      BulkResponse res = Failsafe
          .with(retryPolicy)
          .onFailure((event) -> {
            if (event.isRetry()) {
              MetricsUtil.addHistoricGauge(
                  MetricConstants.OPENSEARCH_BULK_ALL_RETRY_FAILED_COUNT_METRIC, 1);
            }
          })
          .get(() -> {
            requestCount.incrementAndGet();
            BulkResponse response = client.bulk(nextRequest.get(), options);

            // decrease rate if retryable result exceeds threshold; otherwise increase rate
            if (!bulkItemRetryableResultPredicate.test(response)) {
              MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RETRYABLE_RESULT_PERCENTAGE_METRIC, 0);
              rateLimiter.increaseRate();
            } else {
              BulkRequest retryableRequest = getRetryableRequest(nextRequest.get(), response);
              double retryablePercentage = (double) retryableRequest.requests().size() / response.getItems().length;
              // TODO: long type metric
              MetricsUtil.addHistoricGauge(MetricConstants.OS_BULK_RETRYABLE_RESULT_PERCENTAGE_METRIC, (long) (retryablePercentage * 100));

              // TODO: magic number
              if (retryablePercentage > 0.2) {
                rateLimiter.decreaseRate();
              } else {
                rateLimiter.increaseRate();
              }

              if (retryPolicy.getConfig().allowsRetries()) {
                nextRequest.set(retryableRequest);
              }
            }
            return response;
          });
      return res;
    } catch (FailsafeException ex) {
      LOG.severe("Request failed permanently. Re-throwing original exception.");

      // unwrap original exception and throw
      throw new RuntimeException(ex.getCause());
    } finally {
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_SIZE_METRIC, bulkRequest.estimatedSizeInBytes());
      MetricsUtil.addHistoricGauge(MetricConstants.OPENSEARCH_BULK_RETRY_COUNT_METRIC, requestCount.get() - 1);
    }
  }

  private BulkRequest getRetryableRequest(BulkRequest request, BulkResponse response) {
    List<DocWriteRequest<?>> bulkItemRequests = request.requests();
    BulkItemResponse[] bulkItemResponses = response.getItems();
    BulkRequest nextRequest = new BulkRequest()
        .setRefreshPolicy(request.getRefreshPolicy());
    nextRequest.setParentTask(request.getParentTask());
    for (int i = 0; i < bulkItemRequests.size(); i++) {
      if (isItemRetryable(bulkItemResponses[i])) {
        verifyIdMatch(bulkItemRequests.get(i), bulkItemResponses[i]);
        nextRequest.add(bulkItemRequests.get(i));
      }
    }
    LOG.info(String.format("Added %d requests to nextRequest", nextRequest.requests().size()));
    return nextRequest;
  }

  private static void verifyIdMatch(DocWriteRequest<?> request, BulkItemResponse response) {
    if (request.id() != null && !request.id().equals(response.getId())) {
      throw new RuntimeException("id doesn't match: " + request.id() + " / " + response.getId());
    }
  }

  /**
   * A predicate to decide if a BulkResponse is retryable or not.
   */
  private static final CheckedPredicate<BulkResponse> bulkItemRetryableResultPredicate = bulkResponse ->
      bulkResponse.hasFailures() && isRetryable(bulkResponse);

  private static boolean isRetryable(BulkResponse bulkResponse) {
    if (Arrays.stream(bulkResponse.getItems())
        .anyMatch(itemResp -> isItemRetryable(itemResp))) {
      LOG.info("Found retryable failure in the bulk response");
      return true;
    }
    return false;
  }

  private static boolean isItemRetryable(BulkItemResponse itemResponse) {
    return itemResponse.isFailed() && !isCreateConflict(itemResponse);
  }

  private static boolean isCreateConflict(BulkItemResponse itemResp) {
    return itemResp.getOpType() == DocWriteRequest.OpType.CREATE &&
        itemResp.getFailure().getStatus() == RestStatus.CONFLICT;
  }
}
