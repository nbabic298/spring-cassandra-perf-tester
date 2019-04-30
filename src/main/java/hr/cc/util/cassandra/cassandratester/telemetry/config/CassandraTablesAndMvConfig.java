package hr.cc.util.cassandra.cassandratester.telemetry.config;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractReactiveCassandraConfiguration;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DataCenterReplication;
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories;

import java.util.Collections;
import java.util.List;

@Configuration
@EnableReactiveCassandraRepositories("hr.cc.util.cassandra.cassandratester.telemetry")
public class CassandraTablesAndMvConfig extends AbstractReactiveCassandraConfiguration {

    private static final String KEYSPACE = "telemetry_test";

    @Value("${telemetry.cassandra.contact-points}")
    private String contactPoints;


    @Value("${telemetry.cassandra.production-replication}")
    private boolean productionReplication;

    @Override
    protected String getKeyspaceName() {
        return KEYSPACE;
    }

    @Override
    protected QueryOptions getQueryOptions() {
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return queryOptions;
    }

    @Override
    public String[] getEntityBasePackages() {
        return new String[]{"hr.cc.util.cassandra.cassandratester.telemetry.mv.model.table",
        "hr.cc.util.cassandra.cassandratester.telemetry.tablessasi.model",
        "hr.cc.util.cassandra.cassandratester.telemetry.tablessidx.model"};
    }

    @Override
    protected List<CreateKeyspaceSpecification> getKeyspaceCreations() {
        return productionReplication ?

                Collections.singletonList(
                        CreateKeyspaceSpecification.createKeyspace(KEYSPACE)
                                .ifNotExists()
                                .withNetworkReplication(DataCenterReplication.of("DC1", 3L))) :

                Collections.singletonList(
                        CreateKeyspaceSpecification.createKeyspace(KEYSPACE)
                                .ifNotExists()
                                .withSimpleReplication(1));
    }

    @Override
    protected String getContactPoints() {
        return contactPoints;
    }

    @Override
    public SchemaAction getSchemaAction() {
        return SchemaAction.CREATE_IF_NOT_EXISTS;
    }

}
