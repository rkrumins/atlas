/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.hook.events;

import org.apache.atlas.hive.hook.AtlasHiveHookContext;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.notification.hook.HookNotification.EntityDeleteRequestV2;
import org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DropTable extends BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(DropTable.class);

    public DropTable(AtlasHiveHookContext context) {
        super(context);
    }

    @Override
    public List<HookNotificationMessage> getNotificationMessages() throws Exception {
        List<HookNotificationMessage> ret      = null;
        List<AtlasObjectId>           entities = getEntities();

        if (entities != null && CollectionUtils.isNotEmpty(entities)) {
            ret = new ArrayList<>(entities.size());

            for (AtlasObjectId entity : entities) {
                ret.add(new EntityDeleteRequestV2(getUserName(), Collections.singletonList(entity)));
            }
        }

        return ret;
    }

    public List<AtlasObjectId> getEntities() throws Exception {
        List<AtlasObjectId> ret = new ArrayList<>();

        for (Entity entity : getHiveContext().getOutputs()) {
            if (entity.getType() == Entity.Type.TABLE) {
                String dbName = entity.getTable().getDbName();
                String        tblQName = getQualifiedName(entity.getTable());
                AtlasObjectId dbId     = new AtlasObjectId(HIVE_TYPE_TABLE, ATTRIBUTE_QUALIFIED_NAME, tblQName);

                if (context.getFilterEnabledFlag()) {
                    LOG.info("DropTable event in HiveHook: Filter flag is enabled");
                    if (context.getValidEntityFlag(dbName)) {
                        LOG.info("DropTable event in HiveHook: valid entity detected with name " + dbName);
                        context.removeFromKnownTable(tblQName);
                        ret.add(dbId);
                    } else {
                        LOG.info("DropTable event in HiveHook: valid entity detected with name " + dbName);
                    }
                } else {
                    LOG.info("DropTable event in HiveHook: Filter flag is disabled");
                    context.removeFromKnownTable(tblQName);
                    ret.add(dbId);
                }
            }
        }

        return ret;
    }
}
