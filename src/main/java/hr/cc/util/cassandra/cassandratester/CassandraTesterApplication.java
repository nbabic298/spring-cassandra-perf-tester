package hr.cc.util.cassandra.cassandratester;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

@SpringBootApplication
public class CassandraTesterApplication {

	public static void main(String[] args) {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		System.setProperty("reactor.netty.ioWorkerCount", "" + Runtime.getRuntime().availableProcessors() * 2);
		SpringApplication.run(CassandraTesterApplication.class, args);
	}

}
