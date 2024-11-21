/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limiter.BlockingLimiter;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import java.util.Optional;
import java.util.logging.Logger;
import org.opensearch.flint.core.FlintOptions;

public class BulkRequestRateLimiter {
  private static final Logger LOG = Logger.getLogger(BulkRequestRateLimiter.class.getName());
  private BlockingLimiter<Void> rateLimiter;

  public BulkRequestRateLimiter(FlintOptions flintOptions) {
    long bulkRequestRateLimitPerNode = flintOptions.getBulkRequestRateLimitPerNode();
    if (bulkRequestRateLimitPerNode > 0) {
      LOG.info("Setting rate limit for bulk request to " + bulkRequestRateLimitPerNode + "/sec");
      FixedLimit rateLimitAlgorithm = FixedLimit.of((int) bulkRequestRateLimitPerNode);
      // there's a default timeout of 1 hour in BlockingLimiter
      // TODO: for some reason BlockingLimiter doesn't have getLimit() method
      // but the SimpleLimiter inside (or AbstractLimiter) has. BlockingLimiter's delegate is also private.
      this.rateLimiter = BlockingLimiter.wrap(SimpleLimiter.newBuilder().limit(rateLimitAlgorithm).build());
    } else {
      LOG.info("Rate limit for bulk request was not set.");
    }
  }

  // Wait so it won't exceed rate limit.
  // Returns a listener that can be used to release the token.
  // Could also return Optional.empty if rate limit is exceeded (wait till timeout) or thread interrupted.
  // Does nothing if rate limit is not set.
  public Optional<Listener> acquirePermit() {
    if (rateLimiter != null) {
      Optional<Limiter.Listener> blockingListener = rateLimiter.acquire(null);
      return blockingListener.map(Listener::new);
    }
    return Optional.of(new Listener(null));
  }

  public int getLimit() {
    if (rateLimiter != null) {
      return 0; // TODO: no way to get limit
    }
    return 0;
  }

  public class Listener {
    private final Limiter.Listener delegate;

    Listener(Limiter.Listener delegate) {
      this.delegate = delegate;
    }

    public void onSuccess() {
      if (delegate != null) {
        delegate.onSuccess();
      }
    }

    public void onIgnore() {
      if (delegate != null) {
        delegate.onIgnore();
      }
    }

    public void onDropped() {
      if (delegate != null) {
        delegate.onDropped();
      }
    }
  }
}
