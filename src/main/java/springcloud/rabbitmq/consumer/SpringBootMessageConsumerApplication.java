package springcloud.rabbitmq.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import springcloud.rabbitmq.dto.Associate;

@SpringBootApplication
@RestController
@EnableBinding(Sink.class)
public class SpringBootMessageConsumerApplication {
	
	private Map<Long, Associate> recordsReceived = new ConcurrentHashMap<>();
	private final AtomicLong counter = new AtomicLong();
	
	public static void main(String[] args) {
		SpringApplication.run(SpringBootMessageConsumerApplication.class, args);
	}
	
	@RequestMapping("/received")
	public Map<Long, Associate> getRecordsSent() {
		return recordsReceived;
	}
	
	@StreamListener(Sink.INPUT)
	public void receive(Associate assoc) {		
		recordsReceived.put(counter.incrementAndGet(), assoc);
		if (counter.longValue() > 20_000) {
			recordsReceived.clear();
		}
	}
}
