# b-simple-consumer

## Topic
L'applicazione è pensata per ricevere messaggi dal topic **'test'** avente una sola partizione. Il consumatore deve essere lanciato **prima** del produttore.

## Parametri
E' possibile specificare i seguenti parametri mediante il comando `sh esegui parametro_1 parametro_2`
1. Indirizzo e porta del broker
2. Nome del topic

Se non viene specificato nessun parametro in input, i valori di default sono:
1. 10.11.1.101:2181
2. 'test'

