package hr.cc.util.cassandra.cassandratester.telemetry.api.sasi;

import com.datastax.driver.core.utils.UUIDs;
import hr.cc.util.cassandra.cassandratester.service.PartitionService;
import hr.cc.util.cassandra.cassandratester.telemetry.api.resource.TelemetryDataResource;
import hr.cc.util.cassandra.cassandratester.telemetry.api.resource.TelemetryResource;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model.SasiTelemetryByAssetUnit;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model.SasiTelemetryByDevice;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.repo.SasiTelemetryByAssetUnitRepository;
import hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.repo.SasiTelemetryByDeviceRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("telemetry/sasi")
public class SasiAssetUnitTelemetryController {

    private final ModelMapper mapper;

    private final PartitionService partitionService;

    private final SasiTelemetryByDeviceRepository telemetryByDeviceRepository;

    private final SasiTelemetryByAssetUnitRepository telemetryByAssetUnitRepository;

    @PostMapping("/")
    public Mono<Void> saveTelemetry(@RequestBody Mono<TelemetryResource> resource) {
        return resource.map(tr -> {
            SasiTelemetryByAssetUnit telemetryByAssetUnit = mapper.map(tr, SasiTelemetryByAssetUnit.class);
            UUID timeUuid = UUIDs.timeBased();
            telemetryByAssetUnit.getPk().setTimeUuid(timeUuid);

            long partition = Instant.ofEpochMilli(UUIDs.unixTimestamp(timeUuid)).atZone(ZoneId.systemDefault())
                    .toLocalDate().withDayOfMonth(1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond() * 1000;

            telemetryByAssetUnit.getPk().setPartition(partition);
            return telemetryByAssetUnit;
        }).flatMap(telemetryByAssetUnitRepository::save)
                .map(tau -> telemetryByDeviceRepository.save(mapper.map(tau, SasiTelemetryByDevice.class)).subscribe())
                .doOnError(e -> log.error(e.getMessage(), e))
                .then();
    }

    @GetMapping("/v1/{appId}/asset-unit/{assetUnitId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryDataResource.TelemetryReading>>> getV1TelemetryDataByAssetUnit(@PathVariable long appId,
                                                                                                         @PathVariable long assetUnitId,
                                                                                                         @PathVariable long from,
                                                                                                         @PathVariable long to,
                                                                                                         @PathVariable int limit,
                                                                                                         @RequestParam(required = false) List<String> keys) {

        if (keys == null || keys.isEmpty()) {

            return Flux.fromIterable(partitionService.getPartitions(from, to))
                    .parallel().runOn(Schedulers.parallel())
                    .flatMap(p -> telemetryByAssetUnitRepository.findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
                            appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), CassandraPageRequest.first(limit)))
                    .sequential()
                    .collectList()
                    .map(tl -> {
                        long time = System.currentTimeMillis();
                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (SasiTelemetryByAssetUnit telemetryByAssetUnit : tl) {
                            if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                    telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                .build());
                            }
                        }
                        log.info("Transformation in millis: {}", System.currentTimeMillis() - time);
                        return telemetryMap;
                    });

        } else {
            return Flux.just(keys.toArray(String[]::new))
                    .flatMap(key -> Flux.fromIterable(partitionService.getPartitions(from, to))
                            .parallel().runOn(Schedulers.parallel())
                            .flatMap(p -> telemetryByAssetUnitRepository.findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThanAndKey(
                                    appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, CassandraPageRequest.first(limit)).collectList())
                            .sequential()
                            .collectList()
                            .map(tll -> tll.stream().flatMap(List::stream).collect(Collectors.toList()))
                            .map(tl -> {

                                long time = System.currentTimeMillis();
                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (SasiTelemetryByAssetUnit telemetryByAssetUnit : tl) {
                                    if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                            telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                                .add(TelemetryDataResource.TelemetryReading.builder()
                                                        .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                        .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                        .build());
                                    }
                                }

                                log.info("Transformation in millis: {}", System.currentTimeMillis() - time);
                                return telemetryMap;
                            })).collectList().map(m -> m.stream()
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).limit(limit).collect(Collectors.toList()))));

        }

    }

    @GetMapping("/v2/{appId}/asset-unit/{assetUnitId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryDataResource.TelemetryReading>>> getV2TelemetryDataByAssetUnit(@PathVariable long appId,
                                                                                                         @PathVariable long assetUnitId,
                                                                                                         @PathVariable long from,
                                                                                                         @PathVariable long to,
                                                                                                         @PathVariable int limit,
                                                                                                         @RequestParam(required = false) List<String> keys) {


        if (keys == null || keys.isEmpty()) {

            return Flux.fromIterable(partitionService.getPartitions(from, to))
                    .parallel().runOn(Schedulers.parallel())
                    .flatMap(p -> telemetryByAssetUnitRepository.findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
                            appId, assetUnitId, p, UUIDs.startOf(from), UUIDs.endOf(to), CassandraPageRequest.first(limit)))
                    .sequential()
                    .collectList()
                    .publishOn(Schedulers.elastic())
                    .map(tl -> {
                        long time = System.currentTimeMillis();
                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (SasiTelemetryByAssetUnit telemetryByAssetUnit : tl) {
                            if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                    telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                .build());
                            }
                        }
                        log.info("Transformation in millis: {}", System.currentTimeMillis() - time);
                        return telemetryMap;
                    });
        } else {
            return Flux.just(keys.toArray(String[]::new))
                    .flatMap(key -> Flux.fromIterable(partitionService.getPartitions(from, to))
                            .parallel().runOn(Schedulers.parallel())
                            .flatMap(p -> telemetryByAssetUnitRepository.findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThanAndKey(
                                    appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, CassandraPageRequest.first(limit)).collectList())
                            .sequential()
                            .collectList()
                            .publishOn(Schedulers.elastic())
                            .map(tll -> tll.stream().flatMap(List::stream).collect(Collectors.toList()))
                            .map(tl -> {

                                long time = System.currentTimeMillis();
                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (SasiTelemetryByAssetUnit telemetryByAssetUnit : tl) {
                                    if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                            telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                                .add(TelemetryDataResource.TelemetryReading.builder()
                                                        .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                        .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                        .build());
                                    }
                                }

                                log.info("Transformation in millis: {}", System.currentTimeMillis() - time);
                                return telemetryMap;
                            })).collectList().map(m -> m.stream()
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).limit(limit).collect(Collectors.toList()))));
        }

    }

    @GetMapping("/v3/{appId}/asset-unit/{assetUnitId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryDataResource.TelemetryReading>>> getV3TelemetryDataByAssetUnit(@PathVariable long appId,
                                                                                                         @PathVariable long assetUnitId,
                                                                                                         @PathVariable long from,
                                                                                                         @PathVariable long to,
                                                                                                         @PathVariable int limit,
                                                                                                         @RequestParam(required = false) List<String> keys) {

        if (keys == null || keys.isEmpty()) {

            return Flux.fromIterable(partitionService.getPartitions(from, to))
                    .flatMap(p -> telemetryByAssetUnitRepository.findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
                            appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), CassandraPageRequest.first(limit))
                            .collectList(), 1)
                    .scan(new ArrayList<>(), (list, telemetryList) -> {
                        list.addAll(telemetryList);
                        return list;
                    })
                    .takeUntil(list -> list.size() >= limit)
                    .collectList()
                    .map(tll -> {
                        long time = System.currentTimeMillis();
                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (Object telemetryByAssetUnitObj : tll.get(tll.size() - 1)) {
                            SasiTelemetryByAssetUnit telemetryByAssetUnit = (SasiTelemetryByAssetUnit) telemetryByAssetUnitObj;
                            if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                    telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                .build());
                            }
                        }
                        log.info("Transformation in millis: {}", System.currentTimeMillis() - time);
                        return telemetryMap;
                    });

        } else {
            return Flux.just(keys.toArray(String[]::new))
                    .flatMap(key -> Flux.fromIterable(partitionService.getPartitions(from, to))
                            .flatMap(p -> telemetryByAssetUnitRepository.findByPkApplicationIdAndPkAssetUnitIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThanAndKey(
                                    appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, CassandraPageRequest.first(limit))
                                    .collectList(), 1)
                            .scan(new ArrayList<>(), (list, telemetryList) -> {
                                list.addAll(telemetryList);
                                return list;
                            })
                            .takeUntil(list -> list.size() >= limit)
                            .collectList()
                            .map(tll -> tll.stream().flatMap(List::stream).collect(Collectors.toList()))
                            .map(tl -> {

                                long time = System.currentTimeMillis();
                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (Object telemetryByAssetUnitObj : tl) {
                                    SasiTelemetryByAssetUnit telemetryByAssetUnit = (SasiTelemetryByAssetUnit) telemetryByAssetUnitObj;
                                    if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                            telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                                .add(TelemetryDataResource.TelemetryReading.builder()
                                                        .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                        .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                        .build());
                                    }
                                }

                                log.info("Transformation in millis: {}", System.currentTimeMillis() - time);
                                return telemetryMap;
                            })).collectList().map(m -> m.stream()
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).limit(limit).collect(Collectors.toList()))));

        }

    }

}
