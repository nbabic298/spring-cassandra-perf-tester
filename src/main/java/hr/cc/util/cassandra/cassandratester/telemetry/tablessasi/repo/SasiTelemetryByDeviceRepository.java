package hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.repo;

import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model.SasiTelemetryByDevice;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model.SasiTelemetryByDevicePk;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface SasiTelemetryByDeviceRepository extends
        ReactiveCassandraRepository<SasiTelemetryByDevice, SasiTelemetryByDevicePk> {

    Flux<SasiTelemetryByDevice> findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
            long appId,
            long deviceId,
            long partition,
            UUID from,
            UUID to,
            Pageable pageable);

    Flux<SasiTelemetryByDevice> findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThanAndKey(
            long appId,
            long deviceId,
            long partition,
            UUID from,
            UUID to,
            String key,
            Pageable pageable);

}
