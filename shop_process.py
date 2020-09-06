import json
import random
import time
from statemachine import StateMachine, State
from statemachine.mixins import MachineMixin
from statemachine.exceptions import TransitionNotAllowed

baseurl = "https://imply-shop.com"


class SessionMachine(StateMachine):

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
    toExit = exitSession.from_(landingPage, shopPage, detailPage, addToBasket, checkoutPage)
    advance = landingPage.to(shopPage) | shopPage.to(detailPage) | detailPage.to(addToBasket) \
        | addToBasket.to(checkoutPage) | checkoutPage.to(payment) | payment.to(exitSession)


class SessionModel(MachineMixin):
    state_machine_name = 'SessionMachine'

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        super(SessionModel, self).__init__()

    def __repr__(self):
        return "{}({!r})".format(type(self).__name__, self.__dict__)


def main():

    sessionId = 0
    allSessions = []

    while True:
        print('Top of loop')
        print(f'Total elements in list: {len(allSessions)}')
        # With a certain probability, create a new session
        if random.random() < 0.5:
            sessionId += 1
            print(f'--> Creating Session: id {sessionId}')
            newSessionModel = SessionModel(state = 'landingPage', id = sessionId)
            newSession = SessionMachine(newSessionModel)
            allSessions.append(newSession)
        # Pick one of the sessions
        try:
            thisSession = random.choice(allSessions)
            print(f'--> Session id {thisSession.model.id}')
            thisSession.advance()
            print(f'--> Session new state {thisSession.model.state}')
        except IndexError:
            print('--> No sessions to choose from')
        except TransitionNotAllowed:
            # Here we end up when the session was in exit state
            print(f'--> removing session id {thisSession.model.id}')
            allSessions.remove(thisSession)
        time.sleep(random.uniform(0.2, 3.0))
        

if __name__ == "__main__":
    main()
