package hr.cc.util.cassandra.cassandratester.telemetry.mv.repo;

import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table.MvTelemetryByAssetUnitPk;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table.MvTelemetryByAssetUnit;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface MvTelemetryByAssetUnitRepository extends
        ReactiveCassandraRepository<MvTelemetryByAssetUnit, MvTelemetryByAssetUnitPk> {

    Flux<MvTelemetryByAssetUnit> findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
            long appId,
            long assetUnitId,
            long partition,
            UUID from,
            UUID to,
            Pageable pageable);

}
