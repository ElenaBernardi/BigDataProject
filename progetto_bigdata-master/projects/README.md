# In questa cartella è presente il codice e gli script per l'esecuzione dei vari esempi.


## Build  

La costruzione (build, ovvero compilazione e assemblaggio) delle applicazioni 
va fatta applicazione per applicazione, 
utilizzando **Gradle**. 

Per compilare un'applicazione bisogna: 

1. collegarsi con `vagrant ssh` alla macchina virtuale **dev** 
   dell'ambiente [developer](../environments/developer/), su cui sono installati *Java SDK* e *Gradle* 

2. posizionarsi nella cartella principale dell'applicazione di interesse 

3. per compilare e assemblare l'applicazione, usare il comando `gradle build` 


## Esecuzione 

Il risultato della costruzione di un'applicazione 
è in generale composto da uno o più **componenti eseguibili**, 
per eseguire una applicazione eseguire il comando `sh esegui` nella cartella principale dell'applicazione. E' inoltre possibile passare dei parametri mediante il comando `sh esegui parametro_1 parametro_2`: i parametri delle applicazioni sono descritti nelle cartelle delle applicazioni corrispondenti.
Dopo aver fatto partire le tre macchine virtuali che compongono l'ambiente progetto è necessario far partire il broker come descritto in **script broker**. E' poi possibile eseguire, sul nodo consumatore e/o sul nodo produttore, le applicazioni di interesse. In particolar modo solo i progetti **a-simple-producer** e **b-simple-consumer** devono essere eseguiti rispettivamente nella macchina **produttore** e **consumatore**, mentre i progetti **e-simple-filter** e **f-multiple-producers-consumers** possono essere eseguiti in una delle due. 


## Cartelle presenti

* **script_broker**
  In questa cartella sono presenti gli script relativi a Zookeeper e Kafka. 
  1. Nel nodo **broker** eseguire il comando `sh projects/script_broker/servers-script start` per far partire Zookeeper e Kafka. Con il comando `sh projects/script_broker/servers-script stop` vengono interrotti entrambi.
  2. Sempre dal nodo **broker**, ricollegandosi una seconda volta alla macchina, eseguire il comando `sh projects/script_broker/topic-script nuova 1 test` per creare un nuovo topic 'test' con una partizione. E' inoltre possibile cancellare un topic con `sh projects/script_broker/topic-script cancella nomeTopic` o visualizzare la lista dei topic creati mediante `sh projects/script_broker/topic-script lista`
  

* **a-simple-producer**
  un'applicazione che genera e invia un certo numero di messaggi al broker su un certo topic.
  E' pensata per essere eseguita sul nodo **produttore**, quando sul nodo **consumatore** è già in esecuzione l'applicazione **b-simple-consumer**

* **b-simple-consumer** 
  un'applicazione che riceve messaggi in modo sincrono dal broker su un certo topic.
  E' pensata per essere eseguita sul nodo **consumatore**, quando sul nodo **produttore** è in esecuzione l'applicazione **a-simple-producer**

* **e-simple-filter**
  un'applicazione che riceve messaggi da un topic, li trasforma e li invia ad un altro topic. Questa applicazione può essere eseguita sia sul nodo **produttore** che **consumatore**

* **f-multiple-producers-consumers**
  un'applicazione in cui sono creati due produttori e due consumatori per analizzare il concetto di partizionamento dei messaggi. In particolar modo i produttori inviano messaggi al medesimo topic, sul quale sono in ascolto i consumatori iscritti allo stesso gruppo. E' necessario usare un topic con **due partizioni**. Questa applicazione può essere eseguita sia sul nodo **produttore** che **consumatore**
 








