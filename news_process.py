import json
import random
import time
import argparse, sys, logging
from statemachine import StateMachine, State
from statemachine.mixins import MachineMixin
from statemachine.exceptions import TransitionNotAllowed


baseurl = "https://imply-news.com"

# Attribute selector dicts. Key is the attribute value, value is the probability of occurrence.
# Probabilities must add up to 1.

d_channel = { 'social media': 0.3, 'organic search': 0.2, 'paid search': 0.3, 'display': 0.1, 'affiliate': 0.1 } 
d_campaign = { 'fb-1 yoga pants': 0.3, 'fb-2 yoga mat': 0.4, 'af-1 ball': 0.2, 'google-1 ball': 0.1 }
d_product = { 'yoga pants': 0.3, 'yoga mat': 0.4, 'ball': 0.3 }
d_gender = { 'm': 0.5, 'w': 0.6 }
d_age = { '18-25': 0.1, '26-35': 0.1, '36-50': 0.4, '51-60': 0.3, '61+': 0.1 }

# State transitions for the shop

class SessionMachine(StateMachine):

    home = State('Home', initial=True)
    content = State('Content')
    clickbait = State('Clickbait')
    subscribe = State('Subscribe')
    plusContent = State('PlusContent')
    affiliateLink = State('AffiliateLink')
    exitSession = State('ExitSession')

    toHome = home.from_(home, content, clickbait, subscribe, plusContent, affiliateLink)
    toContent = content.from_(home, content, clickbait, subscribe, plusContent, affiliateLink)
    toClickbait = clickbait.from_(home, content, clickbait, subscribe, plusContent, affiliateLink)
    toSubscribe = subscribe.from_(home, content, clickbait, subscribe, plusContent, affiliateLink)
    toPlusContent = plusContent.from_(home, content, clickbait, subscribe, plusContent, affiliateLink)
    toAffiliateLink = affiliateLink.from_(home, content, clickbait, subscribe, plusContent, affiliateLink)
    toExitSession = exitSession.from_(home, content, clickbait, subscribe, plusContent, affiliateLink)

    def on_exit_state(self, s):
        logging.debug(f'BAM!! Change - time now: {time.time()} exiting state: {s}')
        emit(self)

# Model of the shop with attributes

class SessionModel(MachineMixin):
    state_machine_name = 'SessionMachine'

    def url(self):
        return baseurl + '/' + self.state + '/' + self.contentId + '/' + self.subContentId

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        super(SessionModel, self).__init__()

    def __repr__(self):
        return "{}({!r})".format(type(self).__name__, self.__dict__)

# Attribute selector function

def selectAttr(d):

    x = random.random()
    cume = 0.0
    sel = None

    for k, p in d.items():
        cume += p
        if cume >= x:
            sel = k
            break
    return sel

# Output function - write to stdout as JSON, so it can be piped into Kafka

def emit(s):

    m = s.model
    emitRecord = {
        'timestamp' : time.time(),
        'url' : m.url(),
        'state' : m.state,
        'id' : m.id,
        'campaign' : m.campaign,
        'channel' : m.channel,
        'product' : m.product,
        'gender' : m.gender,
        'age' : m.age,
        'amount' : round(m.amount, 2),
        'profit' : round(m.profit, 2)
    }
    print(f'{m.id}|{json.dumps(emitRecord)}')


# --- Main entry point ---

def main():

    logLevel = logging.INFO
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', help='Enable debug logging', action='store_true')
    args = parser.parse_args()

    if args.debug:
        logLevel = logging.DEBUG

    logging.basicConfig(level=logLevel)

    sessionId = 0
    allSessions = []

    while True:
        logging.debug('Top of loop')
        logging.debug(f'Total elements in list: {len(allSessions)}')
        # With a certain probability, create a new session
        if random.random() < 0.5:
            sessionId += 1
            logging.debug(f'--> Creating Session: id {sessionId}')
            salesAmount = random.uniform(10.0, 90.0);
            newSessionModel = SessionModel(
                state = 'landingPage',
                id = sessionId,
                campaign = selectAttr(d_campaign),
                channel = selectAttr(d_channel),
                product = selectAttr(d_product),
                gender = selectAttr(d_gender),
                age = selectAttr(d_age),
                amount = salesAmount,
                profit = salesAmount * random.uniform(0.02, 0.10)
            )
            newSession = SessionMachine(newSessionModel)
            allSessions.append(newSession)
        # Pick one of the sessions
        try:
            thisSession = random.choice(allSessions)
            logging.debug(f'--> Session id {thisSession.model.id}')
            logging.debug(thisSession.model)
            thisSession.advance()
        except IndexError:
            logging.debug('--> No sessions to choose from')
        except TransitionNotAllowed:
            # Here we end up when the session was in exit state
            logging.debug(f'--> removing session id {thisSession.model.id}')
            allSessions.remove(thisSession)
        time.sleep(random.uniform(0.2, 3.0))
        

if __name__ == "__main__":
    main()
