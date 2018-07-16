package multiplePCS;

/*due produttori che inviano messaggi a un topic. Due consumatori di due
 * gruppi diversi che ricevono gli stessi messaggi dallo stesso topic*/

public class Main {
	
	
	
	/**
	 * @param args the command line arguments:
	 * @param args[0] numero di messaggi da inviare ai due consumatori
	 * 


	 */
	
	private static final String GROUP_1 = "group1";
	//private static final String GROUP_2 = "group2";
	private static final String TOPIC_NAME = "test";
	
	
	public static void main (String[] args) {
		int numeroTotaleMessaggi=10;
		String ip_porta = "10.11.1.101:2181";
		
		
		/*accesso ed analisi dei parametri*/
		try {
			numeroTotaleMessaggi = Integer.parseInt(args[0]);
			
		} catch(Exception e) {
			
			
		}
		
		SimpleConsumer c1 = new SimpleConsumer(ip_porta, GROUP_1, TOPIC_NAME, "c1");
		System.out.println("multiplePCS.Main: Creato consumer: " + c1.toString());
		System.out.println("Consumatore 1 in attesa di messaggi...");
		
		SimpleConsumer c2 = new SimpleConsumer(ip_porta, GROUP_1, TOPIC_NAME, "c2");
		System.out.println("multiplePCS.Main: Creato consumer: " + c2.toString());
		System.out.println("Consumatore 2 in attesa di messaggi...");
		
		Thread thread1 = new Thread(c1);
		Thread thread2 = new Thread(c2);
		thread1.start();
		thread2.start();

		SimpleProducer p1 = new SimpleProducer("p1");
		System.out.println("multiplePCS.Main: Creato producer: " + p1.toString());
		SimpleProducer p2 = new SimpleProducer("p2");
		System.out.println("multiplePCS.Main: Creato producer: " + p2.toString());
		
		
		
		
		for (int i=1; i<numeroTotaleMessaggi; i++) {
			String msg = "Messaggio #" + i + " inviato da p1";
        	p1.invia(msg, TOPIC_NAME);
        	System.out.println("p1 invia --> "+msg );
		}
		
		
		for (int i=numeroTotaleMessaggi; i<numeroTotaleMessaggi*2; i++) {
			String msg = "Messaggio #" + i + " inviato da p2";;
        	p2.invia(msg, TOPIC_NAME);
        	System.out.println("p2 invia --> "+msg );
		}
		
		
		
		
	}

}
