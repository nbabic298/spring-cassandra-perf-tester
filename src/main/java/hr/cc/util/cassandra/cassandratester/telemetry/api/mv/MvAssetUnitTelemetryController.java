package hr.cc.util.cassandra.cassandratester.telemetry.api.mv;

import com.datastax.driver.core.utils.UUIDs;
import hr.cc.util.cassandra.cassandratester.service.PartitionService;
import hr.cc.util.cassandra.cassandratester.telemetry.api.resource.TelemetryDataResource;
import hr.cc.util.cassandra.cassandratester.telemetry.api.resource.TelemetryResource;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.mv.MvTelemetryByAssetUnitAndKey;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table.MvTelemetryByAssetUnit;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table.MvTelemetryByDevice;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.repo.MvTelemetryByAssetUnitAndKeyRepository;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.repo.MvTelemetryByAssetUnitRepository;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.repo.MvTelemetryByDeviceRepository;
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

import static hr.cc.util.cassandra.cassandratester.telemetry.api.resource.TelemetryDataResource.TelemetryReading;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("telemetry/mv")
public class MvAssetUnitTelemetryController {

    private final ModelMapper mapper;

    private final PartitionService partitionService;

    private final MvTelemetryByDeviceRepository telemetryByDeviceRepository;

    private final MvTelemetryByAssetUnitRepository telemetryByAssetUnitRepository;

    private final MvTelemetryByAssetUnitAndKeyRepository telemetryByAssetUnitAndKeyRepository;

    @PostMapping("/")
    public Mono<Void> saveTelemetryByAsset(@RequestBody Mono<TelemetryResource> telemetryResource) {
        return telemetryResource.map(tr -> {
            MvTelemetryByAssetUnit telemetryByAssetUnit = mapper.map(tr, MvTelemetryByAssetUnit.class);
            UUID timeUuid = UUIDs.timeBased();
            telemetryByAssetUnit.getPk().setTimeUuid(timeUuid);

            long partition = Instant.ofEpochMilli(UUIDs.unixTimestamp(timeUuid)).atZone(ZoneId.systemDefault())
                    .toLocalDate().withDayOfMonth(1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond() * 1000;

            telemetryByAssetUnit.getPk().setPartition(partition);
            return telemetryByAssetUnit;
        }).flatMap(telemetryByAssetUnitRepository::save)
                .map(tau -> telemetryByDeviceRepository.save(mapper.map(tau, MvTelemetryByDevice.class)).subscribe())
                .doOnError(e -> log.error(e.getMessage(), e))
                .then();
    }

    @GetMapping("/v1/{appId}/asset-unit/{assetUnitId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryReading>>> getV1TelemetryDataByAssetUnit(@PathVariable long appId,
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

                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (MvTelemetryByAssetUnit telemetryByAssetUnit : tl) {
                            if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                    telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                .build());
                            }
                        }

                        return telemetryMap;
                    });

        } else {
            return Flux.just(keys.toArray(String[]::new))
                    .flatMap(key -> Flux.fromIterable(partitionService.getPartitions(from, to))
                            .parallel().runOn(Schedulers.parallel())
                            .flatMap(p -> telemetryByAssetUnitAndKeyRepository.findByAppAndAssetUnitAndPartitionAndTimeWindowAndKey(
                                    appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, limit).collectList())
                            .sequential()
                            .collectList()
                            .map(tll -> tll.stream().flatMap(List::stream).collect(Collectors.toList()))
                            .map(tl -> {

                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (MvTelemetryByAssetUnitAndKey telemetryByAssetUnit : tl) {
                                    if (telemetryMap.get(telemetryByAssetUnit.getPk().getKey()) == null ||
                                            telemetryMap.get(telemetryByAssetUnit.getPk().getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByAssetUnit.getPk().getKey(), v -> new ArrayList<>())
                                                .add(TelemetryDataResource.TelemetryReading.builder()
                                                        .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                        .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                        .build());
                                    }
                                }

                                return telemetryMap;
                            })).collectList().map(m -> m.stream()
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).limit(limit).collect(Collectors.toList()))));
        }

    }

    @GetMapping("/v2/{appId}/asset-unit/{assetUnitId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryReading>>> getV2TelemetryDataByAssetUnit(@PathVariable long appId,
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

                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (MvTelemetryByAssetUnit telemetryByAssetUnit : tl) {
                            if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                    telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                .build());
                            }
                        }

                        return telemetryMap;
                    });
        } else {

            return Flux.just(keys.toArray(String[]::new))
                    .flatMap(key -> Flux.fromIterable(partitionService.getPartitions(from, to))
                            .parallel().runOn(Schedulers.parallel())
                            .flatMap(p -> telemetryByAssetUnitAndKeyRepository.findByAppAndAssetUnitAndPartitionAndTimeWindowAndKey(
                                    appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, limit).collectList())
                            .sequential()
                            .collectList()
                            .publishOn(Schedulers.elastic())
                            .map(tll -> tll.stream().flatMap(List::stream).collect(Collectors.toList()))
                            .map(tl -> {

                                long time = System.currentTimeMillis();
                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (MvTelemetryByAssetUnitAndKey telemetryByAssetUnit : tl) {
                                    if (telemetryMap.get(telemetryByAssetUnit.getPk().getKey()) == null ||
                                            telemetryMap.get(telemetryByAssetUnit.getPk().getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByAssetUnit.getPk().getKey(), v -> new ArrayList<>())
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
    public Mono<Map<String, List<TelemetryReading>>> getV3TelemetryDataByAssetUnit(@PathVariable long appId,
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
                    .scan(new HashMap<String, List<MvTelemetryByAssetUnit>>(), (resultMap, telemetryList) -> {
                        telemetryList.sort(Comparator.comparing(t -> UUIDs.unixTimestamp(t.getPk().getTimeUuid())));
                        for (MvTelemetryByAssetUnit telemetryByAssetUnit : telemetryList) {
                            resultMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>()).add(telemetryByAssetUnit);
                        }
                        return resultMap;
                    })
                    .takeUntil(map -> {
                        boolean allKeyValuesLimitReached = true;
                        if (map.size() > 0) {
                            for (String key : map.keySet()) {
                                if (map.get(key).size() < limit) {
                                    allKeyValuesLimitReached = false;
                                }
                            }
                        } else {
                            allKeyValuesLimitReached = false;
                        }
                        return allKeyValuesLimitReached;
                    })
                    .collectList()
                    .map(tll -> {

                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (String key : tll.get(tll.size() - 1).keySet()) {
                            for (MvTelemetryByAssetUnit telemetryByAssetUnit : tll.get(tll.size() - 1).get(key)) {
                                if (telemetryMap.get(telemetryByAssetUnit.getKey()) == null ||
                                        telemetryMap.get(telemetryByAssetUnit.getKey()).size() < limit) {
                                    telemetryMap.computeIfAbsent(telemetryByAssetUnit.getKey(), v -> new ArrayList<>())
                                            .add(TelemetryDataResource.TelemetryReading.builder()
                                                    .time(UUIDs.unixTimestamp(telemetryByAssetUnit.getPk().getTimeUuid()))
                                                    .value(String.valueOf(telemetryByAssetUnit.getValue()))
                                                    .build());
                                }
                            }
                        }
                        return telemetryMap;
                    });

        } else {

            return Flux.just(keys.toArray(String[]::new))
                    .flatMap(key -> Flux.fromIterable(partitionService.getPartitions(from, to))
                            .flatMap(p -> telemetryByAssetUnitAndKeyRepository.findByAppAndAssetUnitAndPartitionAndTimeWindowAndKey(
                                    appId, assetUnitId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, limit)
                                    .collectList(), 1)
                            .scan(new ArrayList<>(), (list, telemetryList) -> {
                                list.add(telemetryList);
                                return list;
                            })
                            .takeUntil(list -> list.stream().mapToInt(i -> ((List) i).size()).sum() >= limit)
                            .collectList()
                            .map(tlll -> tlll.get(tlll.size() - 1))
                            .map(tll -> {

                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (Object telemetryByAssetUnitAndKeyListObj : tll) {
                                    Collections.reverse((List) telemetryByAssetUnitAndKeyListObj);
                                    for (Object telemetryByAssetUnitAndKeyObj : (List) telemetryByAssetUnitAndKeyListObj) {
                                        MvTelemetryByAssetUnitAndKey telemetryByAssetUnitAndKey = (MvTelemetryByAssetUnitAndKey) telemetryByAssetUnitAndKeyObj;
                                        if (telemetryMap.get(telemetryByAssetUnitAndKey.getPk().getKey()) == null ||
                                                telemetryMap.get(telemetryByAssetUnitAndKey.getPk().getKey()).size() < limit) {
                                            telemetryMap.computeIfAbsent(telemetryByAssetUnitAndKey.getPk().getKey(), v -> new ArrayList<>())
                                                    .add(TelemetryDataResource.TelemetryReading.builder()
                                                            .time(UUIDs.unixTimestamp(telemetryByAssetUnitAndKey.getPk().getTimeUuid()))
                                                            .value(String.valueOf(telemetryByAssetUnitAndKey.getValue()))
                                                            .build());
                                        }
                                    }
                                }

                                return telemetryMap;
                            })).collectList()
                    .map(m -> m.stream()
                            .flatMap(map -> map.entrySet().stream())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (l1, l2) -> Stream.concat(l1.stream(), l2.stream()).limit(limit)
                                            .collect(Collectors.toList()))));
        }

    }

}
