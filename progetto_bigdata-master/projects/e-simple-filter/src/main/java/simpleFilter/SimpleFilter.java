package simpleFilter;

public class SimpleFilter implements Runnable {


	/* nome di questo filter */
	private String name;
	/*topic sorgente*/
	private String topicSource;
	/*topic destinatario*/
	private String topicDestination;
	/*produttore*/
	private SimpleProducer producer;
	/*consumatore*/
	private SimpleConsumer consumer;

	public SimpleFilter (String nome, String zookeeper, String groupId, String topicDestination, String topicSource) {
		this.name = nome;
		this.topicSource = topicSource;
		this.topicDestination = topicDestination;

		this.producer = new SimpleProducer("FProducer "+ this.name);
		this.consumer = new SimpleConsumer(zookeeper, groupId, topicSource, "FConsumer");
	}

	/*legge i records dal topic, li modifica e li ritrasmette*/
	public void processMessage () {
		while(true) {
			String msg = this.consumer.riceviMessaggi();
			if (msg!=null) {
				String tasformedMessage = "***MESSAGGIO_TRASFORMATO*** "+msg;
				this.producer.invia(tasformedMessage, topicDestination);
				System.out.println(producer.toString()+" inviato --> "+tasformedMessage);
			}
		}
	}




	@Override
	public String toString() {
		return "SimpleFilter name "+ this.name+ " topicSorgente: "+this.topicSource+ " topicDestinazione: "+this.topicDestination;
	}

	@Override
	public void run() {
		this.processMessage();

	}

}
