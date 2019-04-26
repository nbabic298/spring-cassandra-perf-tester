package hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.mapping.*;

import static org.springframework.data.cassandra.core.mapping.SASI.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("sasi_telemetry_by_device")
public class SasiTelemetryByDevice {

    @PrimaryKey
    private SasiTelemetryByDevicePk pk;

    @SASI
    @NonTokenizingAnalyzed
    private String key;

    @Column("bool_val")
    private Boolean boolValue;

    @Column("string_val")
    private String textValue;

    @Column("long_val")
    private Long longValue;

    @Column("double_val")
    private Double doubleValue;

    public Object getValue() {
        if (doubleValue != null) {
            return doubleValue;
        } else if (longValue != null) {
            return longValue;
        } else if (boolValue != null) {
            return boolValue;
        } else if (textValue != null) {
            return textValue;
        } else {
            throw new IllegalStateException("Telemetry value cannot be null.");
        }
    }

}

