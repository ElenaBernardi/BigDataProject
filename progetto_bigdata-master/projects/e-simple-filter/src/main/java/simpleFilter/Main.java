package simpleFilter;

public class Main {

	private static final String TOPIC_SOURCE_NAME = "source";
	private static final String TOPIC_DEST_NAME = "filter";
	private static final String BASE_FILTER_NAME = "SF";
	private static final String BASE_PRODUCER_NAME = "SP";
	private static final String BASE_CONSUMER_NAME = "SC";


	public static void main(String[] args) {
		String ip_port = "10.11.1.101:2181";
		String filterName = BASE_FILTER_NAME + "[" + (int)(Math.random()*1000) + "]";
		String producerName = BASE_PRODUCER_NAME + "[" + (int)(Math.random()*1000) + "]";
		String consumerName = BASE_CONSUMER_NAME + "[" + (int)(Math.random()*1000) + "]";
		int numMsgs = 10;

		SimpleProducer producer = new SimpleProducer(producerName);
		System.out.println("simpleFilter.Main: Creato producer: " + producer.toString());

		SimpleConsumer consumer = new SimpleConsumer("10.11.1.101:2181", "1", TOPIC_DEST_NAME, consumerName);
		System.out.println("simpleFilter.Main: Creato consumer: " + consumer.toString());

		SimpleFilter filter = new SimpleFilter(filterName, ip_port, "filter", TOPIC_DEST_NAME, TOPIC_SOURCE_NAME);
		System.out.println("simpleFilter.Main: Creato filtro: "+ filter.toString());
		System.out.println("In attesa di messaggi...");

	
		Thread threadFiltro = new Thread(filter);
		Thread threadConsumatore = new Thread(consumer);
		threadFiltro.start();
		threadConsumatore.start();

		
		/* invio di nomMsgs messaggi */
		for (int i=1; i<=numMsgs; i++) {
			String msg = "Message #" + i + " from " + producerName;
			producer.invia(msg, TOPIC_SOURCE_NAME);
			System.out.println(producer.toString()+" inviato --> "+msg );
		}



		





	}
}
