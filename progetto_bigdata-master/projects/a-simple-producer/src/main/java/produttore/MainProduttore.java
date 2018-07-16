package produttore;

public class MainProduttore {

	public static void main(String[] args) {


		/**
		 * @param args the command line arguments:
		 * @param args[0] il nome del topic
		 * @param args[1] il nome del producer default = produttore
		 * @param args[2] ritardo massimo per la generazione dei messaggi, in ms [opt] default = 100
		 * @param args[3] numero di messaggi inviati default = 10

		 */
		String topicName = null;
		String producerName = null;
		int delay;
		int numeroMessaggi;

		Produttore produttore = new Produttore();

		/* read parameters */

		/* default values */
		producerName = "[" + (int)(Math.random()*1000) + "]";
		topicName = "test";
		delay = 100;
		numeroMessaggi = 10;

		
		/* prova a vedere i parametri specificati dall'utente */
		try {
			topicName = new String(args[0]);
			producerName = new String(args[1]);
			delay = (new Integer(args[2])).intValue();
			numeroMessaggi = (new Integer(args[3])).intValue();
		} catch(Exception e) {
			/* indice fuori dai limiti o altro errore (e.g., conversione) */
			/* ok, ci sono i valori di default */
		}

		System.out.println("Nome del produttore:" +producerName+" - numero msg:" + numeroMessaggi);


		for(int i = 0;i<=numeroMessaggi;i++) {
			String messaggio = "Messaggio: "+ Integer.toString(i)+" inviato da: "+producerName;
			produttore.invia(messaggio,topicName);
			randomSleep(delay/2,delay);
		}
		produttore.chiudi();
		System.out.print("Messaggi inviati");
	}

	/* Introduce un ritardo di esattamente delay millisecondi. */
	private static void sleep(int delay) {
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) {}
	}

	/* Introduce un ritardo casuale, compreso tra minDelay e maxDelay millisecondi. */
	private static void randomSleep(int minDelay, int maxDelay) {
		int delay = (int)(minDelay + Math.random()*(maxDelay-minDelay));
		sleep(delay);
	}	
}