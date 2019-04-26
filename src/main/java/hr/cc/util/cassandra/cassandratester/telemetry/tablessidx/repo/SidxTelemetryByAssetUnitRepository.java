package hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.repo;

import hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.model.SidxTelemetryByAssetUnit;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.model.SidxTelemetryByAssetUnitPk;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface SidxTelemetryByAssetUnitRepository extends
        ReactiveCassandraRepository<SidxTelemetryByAssetUnit, SidxTelemetryByAssetUnitPk> {

    Flux<SidxTelemetryByAssetUnit> findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
            long appId,
            long assetUnitId,
            long partition,
            UUID from,
            UUID to,
            Pageable pageable);

    Flux<SidxTelemetryByAssetUnit> findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThanAndKey(
            long appId,
            long assetUnitId,
            long partition,
            UUID from,
            UUID to,
            String key,
            Pageable pageable);

}
