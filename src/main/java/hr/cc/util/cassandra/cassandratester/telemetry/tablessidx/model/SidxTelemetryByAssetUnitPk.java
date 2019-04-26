package hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.model;

import lombok.Data;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

import java.util.UUID;

@Data
@PrimaryKeyClass
public class SidxTelemetryByAssetUnitPk {

    @PrimaryKeyColumn(name = "application_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private long applicationId;

    @PrimaryKeyColumn(name = "asset_unit_id", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    private long assetUnitId;

    @PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.PARTITIONED)
    private long partition;

    @PrimaryKeyColumn(name = "time_uuid", ordinal = 3, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
    private UUID timeUuid;

}
