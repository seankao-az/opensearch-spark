/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.flint.core.FlintClientImpl;
import org.opensearch.flint.core.storage.FlintOpenSearchStorageClientFactory;
import org.opensearch.flint.core.storage.FlintOpenSearchIndexOperationService;
import org.opensearch.flint.core.storage.FlintOpenSearchMetadataLogService;

/**
 * {@link FlintClient} builder.
 */
public class FlintClientBuilder {

  public static FlintClient build(FlintOptions options) {
    FlintOpenSearchStorageClientFactory storageClientFactory = new FlintOpenSearchStorageClientFactory(options);
    FlintOpenSearchIndexOperationService indexOperationService = new FlintOpenSearchIndexOperationService(options, storageClientFactory);
    FlintOpenSearchMetadataLogService metadataLogService = new FlintOpenSearchMetadataLogService(storageClientFactory, indexOperationService);
    return new FlintClientImpl(
        options,
        indexOperationService,
        metadataLogService);
  }
}
