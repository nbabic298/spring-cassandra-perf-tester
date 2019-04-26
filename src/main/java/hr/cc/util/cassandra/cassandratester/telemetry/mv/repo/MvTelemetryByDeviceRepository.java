package hr.cc.util.cassandra.cassandratester.telemetry.mv.repo;

import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table.MvTelemetryByDevice;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table.MvTelemetryByDevicePk;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface MvTelemetryByDeviceRepository extends
        ReactiveCassandraRepository<MvTelemetryByDevice, MvTelemetryByDevicePk> {

    Flux<MvTelemetryByDevice> findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
            long appId,
            long deviceId,
            long partition,
            UUID from,
            UUID to,
            Pageable pageable);

}
