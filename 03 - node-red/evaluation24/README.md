# Evaluation lab - Node-RED

## Group number: 11

## Group members

- Santarsiero Andrea
- Pola Alberto
- Piscaglia Tommaso

## Description of message flows

The flow consists of three independent branches that communicate through the flow context.

Sensor readings: MQTT input 1 subscribes to "neslabpolimi/smartcity/milan" and forwards each reading to Compute average. This function appends the temperature to a sliding window of the last 10 readings, kept in the node context. It then calculates and stores the average of the window in the flow context. The average is computed even when fewer than 10 readings are available.

Threshold K: MQTT input 2 subscribes to "neslabpolimi/nsds/eval24/k" and passes each value to Set K, which stores it in the flow context. Every new value replaces the previous one.

Coherence check: every minute, the inject node activates the Milan OpenWeather node that retrieves the current temperature in Milan in Celsius. Compute difference subtracts the previously calculated average (retrieved from the flow context) from this temperature. Compare difference then checks whether the absolute difference exceeds K. If it does, it sets the payload to "Temperature data is not coherent!" and MQTT out publishes it on "neslabpolimi/nsds/eval24/alarm". Otherwise, the message is discarded.

Each context variable is written by a single node, which avoids race conditions.

## Extensions 

## Bot info 

- API key: <API_KEY_PLACEHOLDER>
- bot URL: https://t.me/group_11_nodered_lab_bot
