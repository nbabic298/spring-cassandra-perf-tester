package hr.cc.util.cassandra.cassandratester.telemetry.mv.model.mv;

import lombok.Data;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@Table("mv_telemetry_by_device_and_key")
public class MvTelemetryByDeviceAndKey {

    @PrimaryKey
    private MvTelemetryByDeviceAndKeyPk pk;

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
