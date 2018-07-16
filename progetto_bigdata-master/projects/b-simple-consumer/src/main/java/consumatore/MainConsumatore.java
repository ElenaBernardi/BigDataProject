package consumatore;



public class MainConsumatore {
	//il consumatore riceve messaggi solo su un topic
	public static void main(String[] args) {
		/* read parameters */
		String ip_porta = null;
		String topicName = null;
		
		/* default values */
		
		ip_porta = "10.11.1.101:2181";
		topicName = "test";
		
		/* prova a vedere i parametri specificati dall'utente */
		try {
			ip_porta = new String(args[0]);
			topicName = new String(args[1]);
			
		} catch(Exception e) {
			/* indice fuori dai limiti o altro errore (e.g., conversione) */
			/* ok, ci sono i valori di default */
		}
		
		System.out.println("Provo a connettermi...");
		Consumatore consumatore = new Consumatore(ip_porta, "testgroup", topicName);
		System.out.println("Consumatore connesso a "+ip_porta+" sul topic "+topicName);
		System.out.println("In attesa di messaggi...");
		consumatore.riceviMessaggi();
		
	}
	
}
