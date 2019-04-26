package hr.cc.util.cassandra.cassandratester.telemetry.api.resource;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryResource {

    private long applicationId;

    private long assetUnitId;

    private long deviceId;

    private String key;

    private Boolean boolValue;

    private String textValue;

    private Long longValue;

    private Double doubleValue;

}
