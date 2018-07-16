# Broker-Consumatore-Produttore

Questo ambiente di esecuzione è composto da tre macchine virtuali: 
un broker, un consumatore e un prodotture

## Descrizione delle macchine virtuali 

### Broker
La macchina virtuale **broker** 
è pensata per l'esecuzione di Zookeeper e Kafka ed ha il seguente software: 

* Ubuntu 16.04 LTS a 64 bit (by Bento) 

* Oracle Java SDK 

* Kafka

* Zookeeper

Configurazione di rete 

* Indirizzo IP: 10.11.1.101 


  
### Produttore
La macchina virtuale **produttore**  
è pensata per l'esecuzione di semplici applicazioni per la produzione di messaggi ed ha il seguente software: 

* Ubuntu 16.04 LTS a 64 bit (by Bento) 

* Oracle Java SDK 
  
Configurazione di rete 

* Indirizzo IP: 10.11.1.201



### Consumatore
La macchina virtuale **consumatore**  
è pensata per l'esecuzione di semplici applicazioni per il consumo di messaggi ed ha il seguente software: 

* Ubuntu 16.04 LTS a 64 bit (by Bento) 

* Oracle Java SDK 
  
Configurazione di rete 

* Indirizzo IP: 10.11.1.221


