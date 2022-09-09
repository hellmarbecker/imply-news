# imply-news

## imply-news: Data generator for a publisher

This simulates data for a news outlet. It has free and premium content, a subscribe page, clickbait (multi-page content), and affiliate outlinks.

The state machine is controlled by a transition matrix, this implementation does not use an external library.

While the possible states are always the same, different transition matrices can exist (to model the compelling switching event.) The entire configuration is in YAML format and is held in `news_config.yaml`.

The transition matrices are organized as a dictionary, there should be an entry with key `"default"`.

For secure Kafka (Confluent Cloud), you should create a file `news_secret.yml` in the main directory, which contains the bootstrap address and credentials for Confluent Cloud, like so:

    Kafka:
        bootstrap.servers: "<bootstrap server>"
        security.protocol: "SASL_SSL"
        sasl.mechanisms: "PLAIN"
        sasl.username: "<API Key>"
        sasl.password: "<Secret>"
        
Settings in this section correspond directly with Kafka consumer properties.
        
This file is automatically included if it is present. Do not check secrets into Github.
    
### Dependencies

Python packages:
- Faker
- confluent_kafka
- scipy

### Time envelope

The `timeEnvelope` setting must be an array of 24 values, each between 0 and 1000. These are relative data generation frequencies per hour of day. Frequency values will be interpolated using cubic splines, which is what `scipy` is used for.

### Components of the news data generator

`news_process.py`

The generator script proper. It writes its result to Kafka, or in developer mode to standard output.

`news_config.yml`

State values and transition matrices

`news_simulator.sh`

Driver script, handles mode selection.

`admin.crontab`

Crontab for the default user (`admin` on Debian.) This will cause a drop in conversion rates 1x per week for a few hours. Install using `crontab -e`.

`cube-imply-news.json`

This is not strictly part of the project. It shows an example of a Pivot cube configuration with suggested dimensions and measures for the Imply News data set.
