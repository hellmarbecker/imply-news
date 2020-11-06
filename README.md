# imply-shop

## imply-shop: Data generator for clickstream/e-commerce

Generates fake clickstream data for an online shop, simulating shoppers' sessions.

Sessions are modeled by a state machine. The happy flow goes through the state sequence:

    LandingPage
    ShopPage
    DetailPage
    AddToBasket
    CheckoutPage
    Payment
    ExitSession

Once a session reaches state `ExitSession`, it will be removed automatically.

With a certain probability, the session will be abandoned/cancelled before payment. In that case, it will transition directly to state `ExitSession`.

Successful completion of a purchase is indicated by reaching state `Payment`.

Campaign attribution is achieved through the `campaign` (TODO: and `channel`) attributes.

### Data output format

Data is output as key|value, where the key is the session ID and the value is the session state JSON. This is then piped into `kafkacat`.

Example of an output row:

    118|{"timestamp": 1599644882.312883, "url": "https://imply-shop.com/shopPage", "state": "shopPage", "id": 118, "campaign": "af-1 ball", "product": "yoga pants", "gender": "m", "age": "61+", "amount": 19.2, "profit": 1.21}

### Implementation

State machine: see https://pypi.org/project/python-statemachine/

## imply-news: Data generator for a publisher

This simulates data for a news outlet. It has free and premium content, a subscribe page, clickbait (multi-page content), and affiliate outlinks.

The state machine is controlled by a transition matrix, this implementation does not use an external library.

While the possible states are always the same, different transition matrices can exist (to model the compelling switching event.) The entire configuration is in YAML format and is held in `news_config.yaml`.

The transition matrices are organized as a dictionary, there should be an entry with key `"default"`.

You need to create a file `news_secret.sh` in the main directory, and make it executable, which contains the bootstrap address and credentials for Confluent Cloud, like so:

    export CC_BOOTSTRAP="<bootstrap server>"
    export CC_APIKEY="<API Key>"
    export CC_SECRET="<Secret>"
    
### Dependencies

Python packages:
- Faker
- confluent_kafka

### Components of the news data generator

`news_process.py`

The generator script proper. It writes its result to standard output.

`news_config.yml`

State values and transition matrices

`news_simulator.sh`

Driver script, handles mode selection and sends the main script's output to `kafkacat`.

`cube-imply-news.json`

This is not strictly part of the project. It shows an example of a Pivot cube configuration with suggested dimensions and measures for the Imply News data set.
