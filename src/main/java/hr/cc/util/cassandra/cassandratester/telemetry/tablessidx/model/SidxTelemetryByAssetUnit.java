package hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.model;

import lombok.Data;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@Table("sidx_telemetry_by_asset_unit")
public class SidxTelemetryByAssetUnit {

    @PrimaryKey
    private SidxTelemetryByAssetUnitPk pk;

    @Column("device_id")
    private long deviceId;

    @Indexed
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
