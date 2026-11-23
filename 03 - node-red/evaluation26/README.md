# Evaluation lab - Node-RED

## Group number: 11

## Group members

- Santarsiero Andrea
- Pola Alberto
- Piscaglia Tommaso

## Description of message flows

The flow is split into two tabs that share only the co2 threshold variable, stored in the global context.

Configuration: the SET LIMIT inject nodes send the new value to Set threshold, which stores it in the global context without producing any output.

Monitoring: each SAMPLE inject node sends a reading to the Parser function. The parser stores it as the room's latest value in the flow context and updates the room's streak counter: if the reading is above the threshold the counter is incremented, otherwise it is reset to zero. When two consecutives readings from the same room exceed the threshold, the parser generates an alert field to the message. Only alert messages are forwarded to:
- a function that increases the alertCounter in the flow context
- a function that formats the debug string
- a change node connected to MQTT Output.

RESET messages go to the Resetter function that resets flow variables based on the message topic, as required.

REPORT triggers the Report generator, which reads the flow and global values, using "unknown" for rooms without readings, and sends the JSON to MQTT Output and to the Print report function for the debug string.

## Extensions 
