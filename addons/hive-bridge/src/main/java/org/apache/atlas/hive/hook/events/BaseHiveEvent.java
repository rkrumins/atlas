package org.apache.atlas.hive.hook.events;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;

import static org.apache.atlas.hive.bridge.HiveMetaStoreBridge.MILLIS_CONVERT_FACTOR;

public class BaseHiveEvent {
    private static final Logger LOG = LoggerFactory.getLogger(BaseHiveEvent.class);

    public static final String HIVE_TYPE_DB             = "hive_db";
    public static final String HIVE_TYPE_TABLE          = "hive_table";
    public static final String HIVE_TYPE_STORAGEDESC    = "hive_storagedesc";
    public static final String HIVE_TYPE_COLUMN         = "hive_column";
    public static final String HIVE_TYPE_PROCESS        = "hive_process";
    public static final String HIVE_TYPE_COLUMN_LINEAGE = "hive_column_lineage";
    public static final String HIVE_TYPE_SERDE          = "hive_serde";
    public static final String HIVE_TYPE_ORDER          = "hive_order";
    public static final String HDFS_TYPE_PATH           = "hdfs_path";

    public static final String ATTRIBUTE_QUALIFIED_NAME            = "qualifiedName";
    public static final String ATTRIBUTE_NAME                      = "name";
    public static final String ATTRIBUTE_DESCRIPTION               = "description";
    public static final String ATTRIBUTE_OWNER                     = "owner";
    public static final String ATTRIBUTE_CLUSTER_NAME              = "clusterName";
    public static final String ATTRIBUTE_LOCATION                  = "location";
    public static final String ATTRIBUTE_PARAMETERS                = "parameters";
    public static final String ATTRIBUTE_OWNER_TYPE                = "ownerType";
    public static final String ATTRIBUTE_COMMENT                   = "comment";
    public static final String ATTRIBUTE_CREATE_TIME               = "createTime";
    public static final String ATTRIBUTE_LAST_ACCESS_TIME          = "lastAccessTime";
    public static final String ATTRIBUTE_VIEW_ORIGINAL_TEXT        = "viewOriginalText";
    public static final String ATTRIBUTE_VIEW_EXPANDED_TEXT        = "viewExpandedText";
    public static final String ATTRIBUTE_TABLE_TYPE                = "tableType";
    public static final String ATTRIBUTE_TEMPORARY                 = "temporary";
    public static final String ATTRIBUTE_RETENTION                 = "retention";
    public static final String ATTRIBUTE_DB                        = "db";
    public static final String ATTRIBUTE_STORAGEDESC               = "sd";
    public static final String ATTRIBUTE_PARTITION_KEYS            = "partitionKeys";
    public static final String ATTRIBUTE_COLUMNS                   = "columns";
    public static final String ATTRIBUTE_INPUT_FORMAT              = "inputFormat";
    public static final String ATTRIBUTE_OUTPUT_FORMAT             = "outputFormat";
    public static final String ATTRIBUTE_COMPRESSED                = "compressed";
    public static final String ATTRIBUTE_BUCKET_COLS               = "bucketCols";
    public static final String ATTRIBUTE_NUM_BUCKETS               = "numBuckets";
    public static final String ATTRIBUTE_STORED_AS_SUB_DIRECTORIES = "storedAsSubDirectories";
    public static final String ATTRIBUTE_TABLE                     = "table";
    public static final String ATTRIBUTE_SERDE_INFO                = "serdeInfo";
    public static final String ATTRIBUTE_SERIALIZATION_LIB         = "serializationLib";
    public static final String ATTRIBUTE_SORT_COLS                 = "sortCols";
    public static final String ATTRIBUTE_COL_TYPE                  = "type";
    public static final String ATTRIBUTE_COL_POSITION              = "position";
    public static final String ATTRIBUTE_PATH                      = "path";
    public static final String ATTRIBUTE_NAMESERVICE_ID            = "nameServiceId";
    public static final String ATTRIBUTE_INPUTS                    = "inputs";
    public static final String ATTRIBUTE_OUTPUTS                   = "outputs";
    public static final String ATTRIBUTE_OPERATION_TYPE            = "operationType";
    public static final String ATTRIBUTE_START_TIME                = "startTime";
    public static final String ATTRIBUTE_USER_NAME                 = "userName";
    public static final String ATTRIBUTE_QUERY_TEXT                = "queryText";
    public static final String ATTRIBUTE_QUERY_ID                  = "queryId";
    public static final String ATTRIBUTE_QUERY_PLAN                = "queryPlan";
    public static final String ATTRIBUTE_END_TIME                  = "endTime";
    public static final String ATTRIBUTE_RECENT_QUERIES            = "recentQueries";
    public static final String ATTRIBUTE_QUERY                     = "query";
    public static final String ATTRIBUTE_DEPENDENCY_TYPE           = "depenendencyType";
    public static final String ATTRIBUTE_EXPRESSION                = "expression";
    public static final String ATTRIBUTE_ALIASES                   = "aliases";

    public static AtlasObjectId getObjectId(AtlasEntity entity) {
        String        qualifiedName = (String) entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME);
        AtlasObjectId ret           = new AtlasObjectId(entity.getGuid(), entity.getTypeName(), Collections.singletonMap(ATTRIBUTE_QUALIFIED_NAME, qualifiedName));

        return ret;
    }

    public static final Map<Integer, String> OWNER_TYPE_TO_ENUM_VALUE = new HashMap<>();

    public static final long   MILLIS_CONVERT_FACTOR  = 1000;

    static {
        OWNER_TYPE_TO_ENUM_VALUE.put(1, "USER");
        OWNER_TYPE_TO_ENUM_VALUE.put(2, "ROLE");
        OWNER_TYPE_TO_ENUM_VALUE.put(3, "GROUP");
    }

    public static long getTableCreateTime(Table table) {
        return table.getTTable() != null ? (table.getTTable().getCreateTime() * MILLIS_CONVERT_FACTOR) : System.currentTimeMillis();
    }

    public static List<AtlasObjectId> getObjectIds(List<AtlasEntity> entities) {
        final List<AtlasObjectId> ret;

        if (CollectionUtils.isNotEmpty(entities)) {
            ret = new ArrayList<>(entities.size());

            for (AtlasEntity entity : entities) {
                ret.add(getObjectId(entity));
            }
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }

}
