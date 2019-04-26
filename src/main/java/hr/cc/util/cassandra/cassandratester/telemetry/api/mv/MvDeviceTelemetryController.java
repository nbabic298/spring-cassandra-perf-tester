package hr.cc.util.cassandra.cassandratester.telemetry.api.mv;

import com.datastax.driver.core.utils.UUIDs;
import hr.cc.util.cassandra.cassandratester.service.PartitionService;
import hr.cc.util.cassandra.cassandratester.telemetry.api.resource.TelemetryDataResource;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.mv.MvTelemetryByDeviceAndKey;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table.MvTelemetryByDevice;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.repo.MvTelemetryByDeviceAndKeyRepository;
import hr.cc.util.cassandra.cassandratester.telemetry.mv.repo.MvTelemetryByDeviceRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RestController
@AllArgsConstructor
@RequestMapping("telemetry/mv")
public class MvDeviceTelemetryController {

    private final PartitionService partitionService;

    private final MvTelemetryByDeviceRepository telemetryByDeviceRepository;

    private final MvTelemetryByDeviceAndKeyRepository telemetryByDeviceAndKeyRepository;

    @GetMapping("/v1/{appId}/device/{deviceId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryDataResource.TelemetryReading>>> getV1TelemetryDataByDevice(@PathVariable long appId,
                                                                                                      @PathVariable long deviceId,
                                                                                                      @PathVariable long from,
                                                                                                      @PathVariable long to,
                                                                                                      @PathVariable int limit,
                                                                                                      @RequestParam(required = false) List<String> keys) {

        if (keys == null || keys.isEmpty()) {

            return Flux.fromIterable(partitionService.getPartitions(from, to))
                    .parallel().runOn(Schedulers.parallel())
                    .flatMap(p -> telemetryByDeviceRepository.findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
                            appId, deviceId, p, UUIDs.endOf(from), UUIDs.startOf(to), CassandraPageRequest.first(limit)))
                    .sequential()
                    .collectList()
                    .map(tl -> {
                        long time = System.currentTimeMillis();
                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (MvTelemetryByDevice telemetryByDevice : tl) {
                            if (telemetryMap.get(telemetryByDevice.getKey()) == null ||
                                    telemetryMap.get(telemetryByDevice.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByDevice.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByDevice.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByDevice.getValue()))
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
                            .flatMap(p -> telemetryByDeviceAndKeyRepository.findByAppAndDeviceAndPartitionAndTimeWindowAndKey(
                                    appId, deviceId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, limit).collectList())
                            .sequential()
                            .collectList()
                            .map(tll -> tll.stream().flatMap(List::stream).collect(Collectors.toList()))
                            .map(tl -> {

                                long time = System.currentTimeMillis();
                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (MvTelemetryByDeviceAndKey telemetryByDeviceAndKey : tl) {
                                    if (telemetryMap.get(telemetryByDeviceAndKey.getPk().getKey()) == null ||
                                            telemetryMap.get(telemetryByDeviceAndKey.getPk().getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByDeviceAndKey.getPk().getKey(), v -> new ArrayList<>())
                                                .add(TelemetryDataResource.TelemetryReading.builder()
                                                        .time(UUIDs.unixTimestamp(telemetryByDeviceAndKey.getPk().getTimeUuid()))
                                                        .value(String.valueOf(telemetryByDeviceAndKey.getValue()))
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

    @GetMapping("/v2/{appId}/device/{deviceId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryDataResource.TelemetryReading>>> getV2TelemetryDataByDevice(@PathVariable long appId,
                                                                                                      @PathVariable long deviceId,
                                                                                                      @PathVariable long from,
                                                                                                      @PathVariable long to,
                                                                                                      @PathVariable int limit,
                                                                                                      @RequestParam(required = false) List<String> keys) {


        if (keys == null || keys.isEmpty()) {

            return Flux.fromIterable(partitionService.getPartitions(from, to))
                    .parallel().runOn(Schedulers.parallel())
                    .flatMap(p -> telemetryByDeviceRepository.findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
                            appId, deviceId, p, UUIDs.startOf(from), UUIDs.endOf(to), CassandraPageRequest.first(limit)))
                    .sequential()
                    .collectList()
                    .publishOn(Schedulers.elastic())
                    .map(tl -> {
                        long time = System.currentTimeMillis();
                        Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                        for (MvTelemetryByDevice telemetryByDevice : tl) {
                            if (telemetryMap.get(telemetryByDevice.getKey()) == null ||
                                    telemetryMap.get(telemetryByDevice.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByDevice.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByDevice.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByDevice.getValue()))
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
                            .flatMap(p -> telemetryByDeviceAndKeyRepository.findByAppAndDeviceAndPartitionAndTimeWindowAndKey(
                                    appId, deviceId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, limit).collectList())
                            .sequential()
                            .collectList()
                            .publishOn(Schedulers.elastic())
                            .map(tll -> tll.stream().flatMap(List::stream).collect(Collectors.toList()))
                            .map(tl -> {

                                long time = System.currentTimeMillis();
                                Map<String, List<TelemetryDataResource.TelemetryReading>> telemetryMap = new HashMap<>();
                                for (MvTelemetryByDeviceAndKey telemetryByDeviceAndKey : tl) {
                                    if (telemetryMap.get(telemetryByDeviceAndKey.getPk().getKey()) == null ||
                                            telemetryMap.get(telemetryByDeviceAndKey.getPk().getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByDeviceAndKey.getPk().getKey(), v -> new ArrayList<>())
                                                .add(TelemetryDataResource.TelemetryReading.builder()
                                                        .time(UUIDs.unixTimestamp(telemetryByDeviceAndKey.getPk().getTimeUuid()))
                                                        .value(String.valueOf(telemetryByDeviceAndKey.getValue()))
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

    @GetMapping("/v3/{appId}/device/{deviceId}/from/{from}/to/{to}/limit/{limit}")
    public Mono<Map<String, List<TelemetryDataResource.TelemetryReading>>> getV3TelemetryDataByDevice(@PathVariable long appId,
                                                                                                      @PathVariable long deviceId,
                                                                                                      @PathVariable long from,
                                                                                                      @PathVariable long to,
                                                                                                      @PathVariable int limit,
                                                                                                      @RequestParam(required = false) List<String> keys) {


        if (keys == null || keys.isEmpty()) {

            return Flux.fromIterable(partitionService.getPartitions(from, to))
                    .flatMap(p -> telemetryByDeviceRepository.findByPkApplicationIdAndPkDeviceIdAndPkPartitionAndPkTimeUuidGreaterThanAndPkTimeUuidLessThan(
                            appId, deviceId, p, UUIDs.endOf(from), UUIDs.startOf(to), CassandraPageRequest.first(limit))
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
                        for (Object telemetryByDeviceObj : tll.get(tll.size() - 1)) {
                            MvTelemetryByDevice telemetryByDevice = (MvTelemetryByDevice) telemetryByDeviceObj;
                            if (telemetryMap.get(telemetryByDevice.getKey()) == null ||
                                    telemetryMap.get(telemetryByDevice.getKey()).size() < limit) {
                                telemetryMap.computeIfAbsent(telemetryByDevice.getKey(), v -> new ArrayList<>())
                                        .add(TelemetryDataResource.TelemetryReading.builder()
                                                .time(UUIDs.unixTimestamp(telemetryByDevice.getPk().getTimeUuid()))
                                                .value(String.valueOf(telemetryByDevice.getValue()))
                                                .build());
                            }
                        }
                        log.info("Transformation in millis: {}", System.currentTimeMillis() - time);
                        return telemetryMap;
                    });

        } else {

            return Flux.just(keys.toArray(String[]::new))
                    .flatMap(key -> Flux.fromIterable(partitionService.getPartitions(from, to))
                            .flatMap(p -> telemetryByDeviceAndKeyRepository.findByAppAndDeviceAndPartitionAndTimeWindowAndKey(
                                    appId, deviceId, p, UUIDs.endOf(from), UUIDs.startOf(to), key, limit)
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
                                for (Object telemetryByDeviceAndKeyObj : tl) {
                                    MvTelemetryByDeviceAndKey telemetryByDeviceAndKey = (MvTelemetryByDeviceAndKey) telemetryByDeviceAndKeyObj;
                                    if (telemetryMap.get(telemetryByDeviceAndKey.getPk().getKey()) == null ||
                                            telemetryMap.get(telemetryByDeviceAndKey.getPk().getKey()).size() < limit) {
                                        telemetryMap.computeIfAbsent(telemetryByDeviceAndKey.getPk().getKey(), v -> new ArrayList<>())
                                                .add(TelemetryDataResource.TelemetryReading.builder()
                                                        .time(UUIDs.unixTimestamp(telemetryByDeviceAndKey.getPk().getTimeUuid()))
                                                        .value(String.valueOf(telemetryByDeviceAndKey.getValue()))
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
