package hr.cc.util.cassandra.cassandratester.telemetry.mv.repo;

import hr.cc.util.cassandra.cassandratester.telemetry.mv.model.mv.MvTelemetryByAssetUnitAndKey;
import lombok.AllArgsConstructor;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.util.UUID;

@Repository
@AllArgsConstructor
public class MvTelemetryByAssetUnitAndKeyRepository {

    private final ReactiveCassandraTemplate template;

    public Flux<MvTelemetryByAssetUnitAndKey> findByAppAndAssetUnitAndPartitionAndTimeWindowAndKey(long appId,
                                                                                                   long assetUnitId,
                                                                                                   long partition,
                                                                                                   UUID from,
                                                                                                   UUID to,
                                                                                                   String key,
                                                                                                   long limit) {

        return template.select(Query.query(Criteria.where("application_id").is(appId))
                .and(Criteria.where("asset_unit_id").is(assetUnitId))
                .and(Criteria.where("partition").is(partition))
                .and(Criteria.where("time_uuid").gte(from))
                .and(Criteria.where("time_uuid").lte(to))
                .and(Criteria.where("key").is(key))
                .limit(limit), MvTelemetryByAssetUnitAndKey.class);

    }

}
