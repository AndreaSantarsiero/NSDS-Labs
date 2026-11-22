# Evaluation lab - Node-RED

## Group number: 11

## Group members

- Santarsiero Andrea
- Pola Alberto
- Piscaglia Tommaso

## Description of message flows

When a user writes to the bot, the Telegram Receiver node receives the message and forwards it to the Parse function. The parser extracts the city, the kind of request and the day requested, stores them in msg.query and sends the message out of one of its three outputs. Milan requests go to the Milan weather node, Rome requests go to the Rome weather node and unrecognised requests go to the Help message node, which replies with usage examples.

Both OpenWeather nodes pass the results to the Answer function, which takes the entry at index days*8 (about +24h or +48h) and reads either the weather description or the wind speed. Then, it compares this value with the last one stored in the node context under the key kind_city_days, which is shared by all users, and then updates the stored value. It also increments the flow counter for that kind of request and formats the reply. Finally, the reply message is sent to the Telegram Sender.

Every minute an inject node triggers Log Counters, which reads the stored counters, resets them to zero and builds a timestamped line. Store Logs appends that line to a log file.

## Extensions 

## Bot info 

- API key: <API_KEY_PLACEHOLDER>
- bot URL: https://t.me/group_11_nodered_lab_bot
