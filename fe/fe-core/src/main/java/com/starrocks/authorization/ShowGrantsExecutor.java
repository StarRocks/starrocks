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

package com.starrocks.authorization;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.GrantPrivilegeStmt;
import com.starrocks.sql.ast.GrantRevokeClause;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.GrantType;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Executor for SHOW GRANTS statement.
 * Handles the execution logic for showing user or role grants.
 */
public class ShowGrantsExecutor {

    /**
     * Execute SHOW GRANTS statement.
     *
     * @param statement the SHOW GRANTS statement
     * @param context   the connection context
     * @param metaData  the metadata for the result set
     * @return the result set containing grant information
     */
    public ShowResultSet execute(ShowGrantsStmt statement, ConnectContext context, ShowResultSetMetaData metaData) {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            List<List<String>> infos = new ArrayList<>();
            if (statement.getGrantType().equals(GrantType.ROLE)) {
                List<String> granteeRole = authorizationManager.getGranteeRoleDetailsForRole(statement.getGroupOrRole());
                if (granteeRole != null) {
                    infos.add(granteeRole);
                }

                Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                        authorizationManager.getTypeToPrivilegeEntryListByRole(statement.getGroupOrRole());
                infos.addAll(privilegeToRowString(authorizationManager,
                        new GrantRevokeClause(null, statement.getGroupOrRole()), typeToPrivilegeEntryList));
            } else if (statement.getGrantType().equals(GrantType.GROUP)) {
                List<String> granteeRole = authorizationManager.getGranteeRoleForGroup(statement.getGroupOrRole());
                if (!granteeRole.isEmpty()) {
                    GrantRoleStmt grantRoleStmt = new GrantRoleStmt(granteeRole, statement.getGroupOrRole(),
                            GrantType.GROUP, NodePosition.ZERO);

                    infos.add(Lists.newArrayList(statement.getGroupOrRole(), null, AstToSQLBuilder.toSQL(grantRoleStmt)));
                }
            } else {
                UserRef user = statement.getUser();
                UserIdentity userIdentity;
                if (user == null) {
                    userIdentity = context.getCurrentUserIdentity();
                } else {
                    userIdentity = new UserIdentity(user.getUser(), user.getHost(), user.isDomain(), user.isExternal());
                }

                if (!userIdentity.isEphemeral()) {
                    List<String> granteeRole = authorizationManager.getGranteeRoleForUser(userIdentity);

                    if (!granteeRole.isEmpty()) {
                        GrantRoleStmt grantRoleStmt = new GrantRoleStmt(granteeRole,
                                new UserRef(userIdentity.getUser(), userIdentity.getHost(), userIdentity.isDomain()),
                                NodePosition.ZERO);
                        infos.add(Lists.newArrayList(
                                userIdentity.toString(),
                                null,
                                AstToSQLBuilder.toSQL(grantRoleStmt)));
                    }

                    Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                            authorizationManager.getTypeToPrivilegeEntryListByUser(userIdentity);
                    infos.addAll(privilegeToRowString(authorizationManager,
                            new GrantRevokeClause(statement.getUser(), null), typeToPrivilegeEntryList));
                }
            }
            return new ShowResultSet(metaData, infos);
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    /**
     * Convert privilege entries to row strings for display.
     *
     * @param authorizationManager     the authorization manager
     * @param userOrRoleName           the user or role name clause
     * @param typeToPrivilegeEntryList the privilege entries organized by object type
     * @return list of row strings representing the privileges
     * @throws PrivilegeException if there's an error processing privileges
     */
    private List<List<String>> privilegeToRowString(AuthorizationMgr authorizationManager,
                                                    GrantRevokeClause userOrRoleName,
                                                    Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList)
            throws PrivilegeException {
        List<List<String>> infos = new ArrayList<>();
        for (Map.Entry<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntry
                : typeToPrivilegeEntryList.entrySet()) {
            for (PrivilegeEntry privilegeEntry : typeToPrivilegeEntry.getValue()) {
                ObjectType objectType = typeToPrivilegeEntry.getKey();
                String catalogName;
                try {
                    catalogName = getCatalogNameFromPEntry(objectType, privilegeEntry);
                } catch (MetaNotFoundException e) {
                    // ignore this entry
                    continue;
                }
                List<String> info = new ArrayList<>();
                info.add(userOrRoleName.getRoleName() != null ?
                        userOrRoleName.getRoleName() : userOrRoleName.getUser().toString());
                info.add(catalogName);

                GrantPrivilegeStmt grantPrivilegeStmt = new GrantPrivilegeStmt(new ArrayList<>(), objectType.name(),
                        userOrRoleName, null, privilegeEntry.isWithGrantOption());

                grantPrivilegeStmt.setObjectType(objectType);
                ActionSet actionSet = privilegeEntry.getActionSet();
                List<PrivilegeType> privList = authorizationManager.analyzeActionSet(objectType, actionSet);
                grantPrivilegeStmt.setPrivilegeTypes(privList);
                grantPrivilegeStmt.setObjectList(Lists.newArrayList(privilegeEntry.getObject()));

                try {
                    info.add(AstToSQLBuilder.toSQL(grantPrivilegeStmt));
                    infos.add(info);
                } catch (com.starrocks.sql.common.MetaNotFoundException e) {
                    //Ignore the case of MetaNotFound in the show statement, such as metadata being deleted
                }
            }
        }

        return infos;
    }

    /**
     * Get catalog name from privilege entry object.
     *
     * @param objectType     the object type
     * @param privilegeEntry the privilege entry
     * @return the catalog name
     * @throws MetaNotFoundException if catalog is not found
     */
    private String getCatalogNameFromPEntry(ObjectType objectType, PrivilegeEntry privilegeEntry)
            throws MetaNotFoundException {
        if (objectType.equals(ObjectType.CATALOG)) {
            CatalogPEntryObject catalogPEntryObject =
                    (CatalogPEntryObject) privilegeEntry.getObject();
            if (catalogPEntryObject.getId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                return null;
            } else {
                return getCatalogNameById(catalogPEntryObject.getId());
            }
        } else if (objectType.equals(ObjectType.DATABASE)) {
            DbPEntryObject dbPEntryObject = (DbPEntryObject) privilegeEntry.getObject();
            if (dbPEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                return null;
            }
            return getCatalogNameById(dbPEntryObject.getCatalogId());
        } else if (objectType.equals(ObjectType.TABLE)) {
            TablePEntryObject tablePEntryObject = (TablePEntryObject) privilegeEntry.getObject();
            if (tablePEntryObject.getCatalogId() == PrivilegeBuiltinConstants.ALL_CATALOGS_ID) {
                return null;
            }
            return getCatalogNameById(tablePEntryObject.getCatalogId());
        } else {
            return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        }
    }

    /**
     * Get catalog name by catalog ID.
     *
     * @param catalogId the catalog ID
     * @return the catalog name
     * @throws MetaNotFoundException if catalog is not found
     */
    private String getCatalogNameById(long catalogId) throws MetaNotFoundException {
        if (CatalogMgr.isInternalCatalog(catalogId)) {
            return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        }

        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Optional<Catalog> catalogOptional = catalogMgr.getCatalogById(catalogId);
        if (catalogOptional.isEmpty()) {
            throw new MetaNotFoundException("cannot find catalog");
        }

        return catalogOptional.get().getName();
    }
}
