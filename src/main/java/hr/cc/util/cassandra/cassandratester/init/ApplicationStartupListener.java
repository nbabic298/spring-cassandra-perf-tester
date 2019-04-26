package hr.cc.util.cassandra.cassandratester.init;

import lombok.AllArgsConstructor;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

@Component
@AllArgsConstructor
public class ApplicationStartupListener implements ApplicationListener<ContextRefreshedEvent> {

    private static final String MV_PREFIX = "mv_";

    private static final String SASI_PREFIX = "sasi_";

    private static final String SIDX_PREFIX = "sidx_";

    private static final String TELEMETRY_BY_DEVICE_TABLE_NAME = "telemetry_by_device";

    private static final String TELEMETRY_BY_ASSET_UNIT_TABLE_NAME = "telemetry_by_asset_unit";

    private static final String TELEMETRY_BY_DEVICE_AND_KEY_MV_NAME = "mv_telemetry_by_device_and_key";

    private static final String TELEMETRY_BY_ASSET_AND_KEY_MV_NAME = "mv_telemetry_by_asset_unit_and_key";

    private final ReactiveCassandraTemplate cassandraTemplate;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {

        cassandraTemplate.getReactiveCqlOperations()
                .execute(CREATE_TELEMETRY_BY_DEVICE_AND_KEY_MV_SCRIPT).subscribe();
        cassandraTemplate.getReactiveCqlOperations()
                .execute(CREATE_TELEMETRY_BY_ASSET_UNIT_AND_KEY_MV_SCRIPT).subscribe();

        // TODO: See if tables and/or MVs already have this compaction in production
        cassandraTemplate.getReactiveCqlOperations()
                .execute(MessageFormat.format(ALTER_TABLE_COMPACTION, MV_PREFIX + TELEMETRY_BY_DEVICE_TABLE_NAME)).subscribe();
        cassandraTemplate.getReactiveCqlOperations()
                .execute(MessageFormat.format(ALTER_TABLE_COMPACTION, MV_PREFIX + TELEMETRY_BY_ASSET_UNIT_TABLE_NAME)).subscribe();

        cassandraTemplate.getReactiveCqlOperations()
                .execute(MessageFormat.format(ALTER_TABLE_COMPACTION, SASI_PREFIX + TELEMETRY_BY_DEVICE_TABLE_NAME)).subscribe();
        cassandraTemplate.getReactiveCqlOperations()
                .execute(MessageFormat.format(ALTER_TABLE_COMPACTION, SASI_PREFIX + TELEMETRY_BY_ASSET_UNIT_TABLE_NAME)).subscribe();

        cassandraTemplate.getReactiveCqlOperations()
                .execute(MessageFormat.format(ALTER_TABLE_COMPACTION, SIDX_PREFIX + TELEMETRY_BY_DEVICE_TABLE_NAME)).subscribe();
        cassandraTemplate.getReactiveCqlOperations()
                .execute(MessageFormat.format(ALTER_TABLE_COMPACTION, SIDX_PREFIX + TELEMETRY_BY_ASSET_UNIT_TABLE_NAME)).subscribe();

    }

    private static final String CREATE_TELEMETRY_BY_DEVICE_AND_KEY_MV_SCRIPT =
            "CREATE MATERIALIZED VIEW IF NOT EXISTS "
                    + TELEMETRY_BY_DEVICE_AND_KEY_MV_NAME
                    + " AS SELECT * FROM " + MV_PREFIX + TELEMETRY_BY_DEVICE_TABLE_NAME
                    + " WHERE application_id IS NOT NULL AND device_id IS NOT NULL AND partition IS NOT NULL AND key"
                    + " IS NOT NULL AND time_uuid IS NOT NULL "
                    + " PRIMARY KEY ((application_id, device_id, partition, key), time_uuid)"
                    + " WITH CLUSTERING ORDER BY (time_uuid DESC) AND"
                    + " compaction ="
                    + " {'class' : 'TimeWindowCompactionStrategy',"
                    + " 'compaction_window_size' :  86400000};";

    private static final String CREATE_TELEMETRY_BY_ASSET_UNIT_AND_KEY_MV_SCRIPT =
            "CREATE MATERIALIZED VIEW IF NOT EXISTS "
                    + TELEMETRY_BY_ASSET_AND_KEY_MV_NAME
                    + " AS SELECT * FROM " + MV_PREFIX + TELEMETRY_BY_ASSET_UNIT_TABLE_NAME
                    + " WHERE application_id IS NOT NULL AND asset_unit_id IS NOT NULL AND partition IS NOT NULL AND key"
                    + " IS NOT NULL AND time_uuid IS NOT NULL "
                    + " PRIMARY KEY ((application_id, asset_unit_id, partition, key), time_uuid)"
                    + " WITH CLUSTERING ORDER BY (time_uuid DESC) AND"
                    + " compaction ="
                    + " {'class' : 'TimeWindowCompactionStrategy',"
                    + " 'compaction_window_size' :  86400000};";

    private static final String ALTER_TABLE_COMPACTION =
            "ALTER TABLE {0} " +
                    "  WITH compaction =" +
                    "  '{' ''class'' : ''TimeWindowCompactionStrategy''," +
                    "   ''compaction_window_size'' :  86400000 '}';";

}
