# a-simple-producer

## Topic
L'applicazione è pensata per inviare messaggi al topic **'test'** avente una sola partizione. Su tale topic deve essere già in ascolto un consumatore.

## Parametri
E' possibile specificare i seguenti parametri mediante il comando `sh esegui parametro_1 parametro_2`
1. Nome del topic
2. Nome del producer
3. Ritardo massimo per la generazione dei messaggi, espresso in ms
4. Numero di messaggi inviati

Se non viene specificato **nessun parametro** in input, i valori di default sono:
1. 'test'
2. Intero random
3. 100
4. 10
