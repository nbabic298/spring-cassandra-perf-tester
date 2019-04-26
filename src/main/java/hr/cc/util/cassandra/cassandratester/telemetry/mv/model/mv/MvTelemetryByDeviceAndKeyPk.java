package hr.cc.util.cassandra.cassandratester.telemetry.mv.model.mv;

import lombok.Data;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

import java.util.UUID;

@Data
@PrimaryKeyClass
public class MvTelemetryByDeviceAndKeyPk {

    @PrimaryKeyColumn(name = "application_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private long applicationId;

    @PrimaryKeyColumn(name = "device_id", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    private long deviceId;

    @PrimaryKeyColumn(ordinal = 2, type = PrimaryKeyType.PARTITIONED)
    private long partition;

    @PrimaryKeyColumn(ordinal = 3, type = PrimaryKeyType.PARTITIONED)
    private String key;

    @PrimaryKeyColumn(name = "time_uuid", ordinal = 4, type = PrimaryKeyType.CLUSTERED, ordering = Ordering.DESCENDING)
    private UUID timeUuid;

}
