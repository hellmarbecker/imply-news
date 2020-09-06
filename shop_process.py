import json
import random
import time
from StateMachine import StateMachine, State

baseurl = "https://imply-shop.com"

class Session(StateMachine):

    landingPage = State('LandingPage', initial=True)
    shopPage = State('ShopPage')
    detailPage = State('DetailPage')
    addToBasket = State('AddToBasket')
    checkoutPage = State('CheckoutPage')
    payment = State('Payment')
    exitSession = State('ExitSession')

    toShop = landingPage.to(shopPage)
    toDetail = shopPage.to(detailPage)
    toBasket = detailPage.to(addToBasket)
    toCheckout = addToBasket.to(checkoutPage)
    toPayment = checkoutPage.to(payment)
    toExit = landingPage.to(exitSession) | shopPage.to(exitSession) | detailPage.to(exitSession) |
        addToBasket.to(exitSession) | checkoutPage.to(exitSession) | payment.to(exitSession)

def main():

    while True:
        # With a certain probability, create a new session
        if random.random() < 0.05:
            newSession = Session()
            allSessions.append(newSession)
        # Pick one of the sessions
        thisSession = random.choice(allSessions)
        thisSession.advance()
        

if __name__ == "__main__":
    main()
