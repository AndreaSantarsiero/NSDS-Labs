# Evaluation lab - Node-RED

## Group number: 11

## Group members

- Santarsiero Andrea
- Pola Alberto
- Piscaglia Tommaso

## Description of message flows

When a user writes to the bot, the Telegram Receiver node receives the message and forwards it to the Parse function. The parser splits the message into type, city and country, stores them in msg.query and routes the message to one of its three outputs: QUERY/REPORT, TRACK or unrecognised. Unrecognised commands go to the Help node, which replies with usage examples.

For TRACK messages, the TRACK answer function adds the location to the user's list in the flow context using the Telegram user ID as key and avoiding duplicates. It then prepares the confirmation answer.

QUERY messages are treated as REPORT messages with a single location. Get cities sets msg.payload to the requested location (QUERY) or to the user's tracked list read from the flow context (REPORT). Split cities emits one message per location. Prepare request copies each location into msg.location, and the OpenWeather node retrieves its current temperature. Prepare answer formats one line per city. Join results automatically collects all lines of the same request in their original order, and Merge results joins them into one answer.

All replies pass through the Answer message node and then to the Telegram Sender.

## Extensions

## Bot info 

- API key: <API_KEY_PLACEHOLDER>
- bot URL: https://t.me/group_11_nodered_lab_bot
