# Apache Kafka tutorial

Tutorial and examples of Apache Kafka for the NSDS course at Politecnico di Milano

## Running

To run any program, you need Java, Maven and a running instance of Kafka.

First compile everything

```bash
mvn clean compile
```

Then you can start any program with the corresponding `start` script for your OS.
You need to specify the main class path and optionnaly the running arguments.

Note that for `it.polimi.middleware.kafka.` is prepended to the main class path.

For example on Linux for TopicManager

```bash
./start.sh admin.TopicManager topicA
```

To gracefully shutdown a running program, you can press `Enter`.
In most cases it should stop the program and close connections, avoiding weird errors
for the next run of the same program.

## Exercises

For each exercise you can find the list of command to execute, most of the time each in a separate shell.
You need to compile the project at least once, see above.

### 1

We added a third consumer to read the messages produced (last command).
```bash
./start.sh basic.BasicProducer topicA
./start.sh basic.BasicConsumer groupA topicA
./start.sh es1.ProcessorForwarder groupB topicA topicB
./start.sh basic.BasicConsumer groupA topicB
```
