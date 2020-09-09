# imply-shop

## Data generator for clickstream/e-commerce

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

## Data output format

Data is output as key|value, where the key is the session ID and the value is the session state JSON. This is then piped into `kafkacat`.

Example of an output row:

    118|{"timestamp": 1599644882.312883, "url": "https://imply-shop.com/shopPage", "state": "shopPage", "id": 118, "campaign": "af-1 ball", "product": "yoga pants", "gender": "m", "age": "61+", "amount": 19.2, "profit": 1.21}

## Implementation

State machine: see https://pypi.org/project/python-statemachine/
