package hr.cc.util.cassandra.cassandratester.telemetry.api.resource;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryDataResource {

    private String key;

    private List<TelemetryReading> readings;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TelemetryReading {

        private long time;
        private String value;

    }

}
