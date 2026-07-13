// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.http.rest.context;

import com.starrocks.http.ActionController;
import com.starrocks.http.IllegalArgException;

/**
 * Aggregator for the semantic-context REST surface. Keeping all registrations in one class lets
 * {@code HttpServer} wire the whole family with a single call, matching the pattern used for other
 * cross-cutting rest families (e.g. meta service, proc profile).
 */
public final class ContextRestActions {

    private ContextRestActions() {
    }

    public static void registerActions(ActionController controller) throws IllegalArgException {
        CreateContextBaseAction.registerAction(controller);
        ListContextBasesAction.registerAction(controller);
        DropContextBaseAction.registerAction(controller);
        CreateCollectionAction.registerAction(controller);
        ListCollectionsAction.registerAction(controller);
        CreateWorkspaceAction.registerAction(controller);
        ListWorkspacesAction.registerAction(controller);
        CreateRetrievalProfileAction.registerAction(controller);
        ContextUpsertAction.registerAction(controller);
        ContextUpdateMetadataAction.registerAction(controller);
        ContextGetAction.registerAction(controller);
        ContextDeleteAction.registerAction(controller);
        ContextHistoryAction.registerAction(controller);
        WorkspaceUpsertAction.registerAction(controller);
        ContextSearchAction.registerAction(controller);
        ContextGraphExpandAction.registerAction(controller);
        ContextPackAction.registerAction(controller);
        ContextTextSearchAction.registerAction(controller);
        ContextVectorSearchAction.registerAction(controller);
        ContextReadCollectionAction.registerAction(controller);
        ContextReadContextBaseAction.registerAction(controller);
        ContextBulkImportAction.registerAction(controller);
        ContextBulkDeleteAction.registerAction(controller);
        ContextHealthAction.registerAction(controller);
        ContextHealthBasicAction.registerAction(controller);
        ContextStatsAction.registerAction(controller);
        StartWorkspaceAction.registerAction(controller);
        DiscardWorkspaceAction.registerAction(controller);
    }
}
