package hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.repo;

import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model.SasiTelemetryByAssetUnit;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model.SasiTelemetryByAssetUnitPk;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface SasiTelemetryByAssetUnitRepository extends
        ReactiveCassandraRepository<SasiTelemetryByAssetUnit, SasiTelemetryByAssetUnitPk> {

    Flux<SasiTelemetryByAssetUnit> findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
            long appId,
            long assetUnitId,
            long partition,
            UUID from,
            UUID to,
            Pageable pageable);

    Flux<SasiTelemetryByAssetUnit> findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThanAndKey(
            long appId,
            long assetUnitId,
            long partition,
            UUID from,
            UUID to,
            String key,
            Pageable pageable);

}
