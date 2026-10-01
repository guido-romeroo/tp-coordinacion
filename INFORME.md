# Informe:

## 1. Visión general

El sistema procesa los mensajes de cada cliente en tres etapas:

1. **Sum** (`SUM_AMOUNT` instancias): procesan las frutas, cantidad que llegan de la cola de mensajes del gateway. Cuando reciben una fruta, calculan, para cada cliente, la suma actual de esta.
2. **Aggregation** (`AGGREGATION_AMOUNT` instancias): reciben las sumas de las frutas de todos los Sum, calculan el top de frutas parcial para cada cliente y lo envían al join para que calcule el top definitivo.

El desafío central es que **ninguna instancia de Sum sabe, excepto la que recibe el eof original, cuándo terminó de procesarse el último mensaje de un cliente**, porque los mensajes se reparten entre todas. Para resolverlo, las instancias de Sum se coordinan con un anillo lógico sobre un exchange de control.

## 2. Coordinación entre instancias de Sum

Cada Sum tiene dos fuentes de mensajes: la cola del gateway (datos y EOF original del cliente) y su cola de control (routing key `SUM_CONTROL_EXCHANGE_<ID>`). Ambas se escuchan en hilos separados y se procesan de forma secuencial en el hilo principal.

### Algoritmo del EOF

El EOF del cliente trae el **total de mensajes esperado** y la lista de ids ya finalizados (vacía al principio).

1. **Recepción del EOF original.** El Sum que lo saca de la cola del cliente es quien "arranca" el protocolo. Se cuenta a sí mismo en la lista de finalizados porque no necesita participar del anillo, ya que ya sabe que se procesaron todas las frutas. **Este Sum no participa del anillo**: queda excluido porque su id está en la lista de finalizados.
2. **Circulación en el anillo.** Si todavía faltan mensajes, el EOF se envía al siguiente Sum del anillo lógico por el exchange de control. Cada Sum recibe el total y resta lo que procesó, luego reinicia su contador y reenvía el EOF al siguiente.
3. **Llegada a cero.** El Sum que deja el faltante en 0 sabe que se procesaron todos los mensajes del cliente. Entonces hace un **broadcast** del EOF (con faltante 0) a todos sus compañeros.
4. **Cierre.** Cada Sum, al recibir el EOF con faltante 0, envía sus sumas parciales a los Aggregation, envía el EOF a todos los Aggregation y libera el estado de ese cliente.

## 3. Coordinación entre Sum y Aggregation

- **Particionado por fruta.** Cada suma parcial se envía al Aggregation que corresponde a `hash(fruta) % AGGREGATION_AMOUNT` (FNV-1a). Una misma fruta siempre llega al mismo Aggregation, sin importar de qué Sum provenga. Así el total de cada fruta se compone completo en un único lugar y el top parcial de cada Aggregation es correcto.
- **EOF a todos.** Como cada Aggregation puede tener frutas de cualquier cliente, cada Sum envía su EOF a **todos** los Aggregation.
- **Cierre del cliente.** Cada Aggregation cuenta los EOF por cliente y recién al recibir `SUM_AMOUNT` calcula su top (`TOP_SIZE`) y lo envía a la cola del join. 

Dentro de un mismo Sum, los datos de un cliente se publican antes que su EOF, por lo que cuando un Aggregation recibe el EOF de un Sum ya recibió todas las sumas de ese Sum.
- **Salida.** Cada Aggregation emite un top parcial por cliente; la combinación de los `AGGREGATION_AMOUNT` tops es responsabilidad del join.

## 4. Escalabilidad

### Respecto a los clientes
- El estado se indexa por `client_id` (sumas, contadores de procesados, tops, contadores de EOF), por lo que varios clientes se procesan **en simultáneo** sin interferirse.
- El estado de un cliente se libera al cerrar su EOF, así que la memoria depende de los clientes concurrentes y no de los históricos.
- Costo: la memoria crece linealmente con la cantidad de clientes simultáneos y de frutas distintas por cliente.

### Respecto a grandes volúmenes de datos
- **Sum escala horizontalmente** agregando instancias: la cola compartida reparte los mensajes (consumidores competitivos).
- Cada Sum guarda **una suma por fruta**, no los mensajes. La memoria depende de la cantidad de frutas distintas y no del volumen de entrada.
- Al cerrar, cada Sum envía un mensaje por fruta distinta y no uno por mensaje recibido.
- **Aggregation escala por particionado**: cada instancia solo mantiene `1/AGGREGATION_AMOUNT`.

### Respecto a la cantidad de controles
- El costo de coordinación **no depende del volumen de datos**, sino de la cantidad de instancias.
- Por cliente, el EOF recorre como mínimo las `SUM_AMOUNT - 1` instancias del anillo, más un broadcast de `SUM_AMOUNT - 1` mensajes al cerrar. Es decir, crece **linealmente con `SUM_AMOUNT`**.
- Hacia Aggregation se envían `SUM_AMOUNT × AGGREGATION_AMOUNT` EOF por cliente.
- Si algún Sum todavía no procesó sus mensajes pendientes, el EOF puede dar más de una vuelta al anillo. 

## 5. Limitaciones conocidas

- **Vueltas sin espera.** Si faltan mensajes, el EOF circula sin pausa entre nodos.