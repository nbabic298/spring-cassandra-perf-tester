package hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.repo;

import hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.model.SidxTelemetryByDevice;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.model.SidxTelemetryByDevicePk;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface SidxTelemetryByDeviceRepository extends
        ReactiveCassandraRepository<SidxTelemetryByDevice, SidxTelemetryByDevicePk> {

    Flux<SidxTelemetryByDevice> findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
            long appId,
            long deviceId,
            long partition,
            UUID from,
            UUID to,
            Pageable pageable);

    Flux<SidxTelemetryByDevice> findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThanAndKey(
            long appId,
            long deviceId,
            long partition,
            UUID from,
            UUID to,
            String key,
            Pageable pageable);

}
