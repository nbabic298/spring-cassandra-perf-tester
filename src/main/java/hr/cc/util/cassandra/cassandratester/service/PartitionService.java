package hr.cc.util.cassandra.cassandratester.service;

import hr.cc.util.cassandra.cassandratester.service.support.PartitionType;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Service
public class PartitionService {

    /**
     * Default partition value is month.
     */
    public List<Long> getPartitions(long from, long to) {
        long firstPartition = Instant.ofEpochMilli(from).atZone(ZoneId.systemDefault())
                .toLocalDate().withDayOfMonth(1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond() * 1000;

        long lastPartition = Instant.ofEpochMilli(to).atZone(ZoneId.systemDefault())
                .toLocalDate().withDayOfMonth(1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond() * 1000;

        List<Long> partitions = new ArrayList<>();
        long partition = firstPartition;
        while (partition <= lastPartition) {
            partitions.add(partition);
            partition = Instant.ofEpochMilli(partition).atZone(ZoneId.systemDefault())
                    .toLocalDate().plusMonths(1).withDayOfMonth(1).atStartOfDay(ZoneId.systemDefault()).toEpochSecond() * 1000;
        }
        return partitions;
    }

    public List<Long> getPartitions(long from, long to, PartitionType type) {
        // TODO: later
        return new ArrayList<>();
    }

}
