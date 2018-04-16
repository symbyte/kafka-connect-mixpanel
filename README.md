# kafka-connect-mixpanel
The connector loads your events data from Mixpanel to Kafka. 
Currently the SourceRecords created have a simple **STRING_SCHEMA**.

# Building
You can build the connector with Maven:
```
mvn clean
mvn package
```

## Sample Configuration
```ini
name=mixpanel-connector
connector.class=org.apache.kafka.connect.mixpanel.MixPanelConnector
tasks.max=1
api_key = YOUR_MIXPANEL_API_KEY
api_secret = YOUR_MIXPANEL_SECRET_KEY
poll_frequency=2
update_window=1
topic=mixp
```
* **name**: name of the connector.
* **connector.class**: class of the implementation of the connector.
* **tasks.max**: maximum number of tasks to create. Even if you put a number greater than one here it will be ignored by the implementation.
* **api_key**: your API key for the project from which you want to pull data from.
* **api_secret**: your API secret for the project from which you want to pull data from.
* **poll_frequency**: how often to ask the mixpanel api for more data (in hours)
* **update_window**: how far back to request data from the current date (in days)
* **topic**: the name of the Kafka topic where the data will be pushed.

Due to limitations of the API coupled with the desire to have the most current data possible, it is necessary to use this gradually-expanding-window approach when polling the mixpanel api. This approach will create **many duplicate events** depending on how you set the `poll_frequency` and `update_window`. Consumers of the topic being published to should have a strategy for dealing with this.


Made with &#9829; from the [Blendo](https://www.blendo.co/) team
