import yaml
import json
import random
import time
import argparse, sys, logging
from faker import Faker
from exceptions import InvalidStateException, InvalidTransitionException

baseurl = "https://imply-news.com"
fake = Faker()

# Attribute selector dicts. Key is the attribute value, value is the probability of occurrence.
# Probabilities must add up to 1.

d_channel = { 'social media': 0.3, 'organic search': 0.2, 'paid search': 0.3, 'display': 0.1, 'affiliate': 0.1 } 
d_campaign = { 'fb-1 Be Informed': 0.3, 'fb-2 US Election': 0.4, 'af-1 Latest News': 0.2, 'google-1 Be Informed': 0.1 }
d_gender = { 'm': 0.5, 'w': 0.6 }
d_age = { '18-25': 0.1, '26-35': 0.1, '36-50': 0.4, '51-60': 0.3, '61+': 0.1 }
l_content = "News Comment World Business Sport Puzzle Law".split()

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

# State transitions for the shop

States = [ 'home', 'initial', 'content', 'clickbait', 'subscribe', 'plusContent', 'affiliateLink', 'exitSession' ]

# from -> to transition probabilities

StateTransitionMatrix = {
    'home':          { 'home': 0.10, 'content': 0.30, 'clickbait': 0.06, 'subscribe': 0.02, 'plusContent': 0.20, 'affiliateLink': 0.02, 'exitSession': 0.30 },
    'content':       { 'home': 0.15, 'content': 0.45, 'clickbait': 0.06, 'subscribe': 0.02, 'plusContent': 0.20, 'affiliateLink': 0.02, 'exitSession': 0.10 },
    'clickbait':     { 'home': 0.10, 'content': 0.10, 'clickbait': 0.50, 'subscribe': 0.02, 'plusContent': 0.16, 'affiliateLink': 0.02, 'exitSession': 0.10 },
    'subscribe':     { 'home': 0.10, 'content': 0.20, 'clickbait': 0.00, 'subscribe': 0.02, 'plusContent': 0.50, 'affiliateLink': 0.02, 'exitSession': 0.16 },
    'plusContent':   { 'home': 0.10, 'content': 0.30, 'clickbait': 0.06, 'subscribe': 0.02, 'plusContent': 0.40, 'affiliateLink': 0.02, 'exitSession': 0.10 },
    'affiliateLink': { 'home': 0.20, 'content': 0.20, 'clickbait': 0.00, 'subscribe': 0.00, 'plusContent': 0.05, 'affiliateLink': 0.00, 'exitSession': 0.55 }
}


class Session:
        
    def __init__(self, states, initialState, stateTransitionMatrix, **kwargs):
        if initialState not in states:
            raise InvalidStateException()
        self.states = States
        self.state = initialState
        self.stateTransitionMatrix = StateTransitionMatrix
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return "{}({!r})".format(type(self).__name__, self.__dict__)

    def advance(self):
        newState = selectAttr(self.stateTransitionMatrix[self.state])
        if newState is None:
            raise InvalidTransitionException()
        emit(self)
        logging.debug(f'advance(): from {self.state} to {newState}')
        self.state = newState

    def url(self):
        return baseurl + '/' + self.state + '/' + self.contentId + '/' + self.subContentId


# Output function - write to stdout as JSON, so it can be piped into Kafka

def emit(s):

    emitRecord = {
        'timestamp' : time.time(),
        'url' : s.url(),
        'state' : s.state,
        'sid' : s.sid,
        'campaign' : s.campaign,
        'channel' : s.channel,
        'contentId' : s.contentId,
        'subContentId' : s.subContentId,
        'gender' : s.gender,
        'age' : s.age
    }
    print(f'{s.sid}|{json.dumps(emitRecord)}')


# --- Main entry point ---

def main():

    logLevel = logging.INFO
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', help='Enable debug logging', action='store_true')
    parser.add_argument('-q', '--quiet', help='Quiet mode (overrides Debug mode)', action='store_true')
    parser.add_argument('-f', '--config', help='Configuration file for session state machine(s)', required=True)
    args = parser.parse_args()

    config = args.config
    if args.debug:
        logLevel = logging.DEBUG
    if args.quiet:
        logLevel = logging.ERROR

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
            newSession = Session(
                States, 'home', StateTransitionMatrix,
                sid = sessionId,
                campaign = selectAttr(d_campaign),
                channel = selectAttr(d_channel),
                contentId = random.choice(l_content),
                subContentId = fake.text(20),
                gender = selectAttr(d_gender),
                age = selectAttr(d_age)
            )
            allSessions.append(newSession)
        # Pick one of the sessions
        try:
            thisSession = random.choice(allSessions)
            logging.debug(f'--> Session id {thisSession.sid}')
            logging.debug(thisSession)
            thisSession.advance()
            if not args.quiet:
                sys.stderr.write('.')
                sys.stderr.flush()
        except IndexError:
            logging.debug('--> No sessions to choose from')
        except KeyError:
            # Here we end up when the session was in exit state
            logging.debug(f'--> removing session id {thisSession.sid}')
            allSessions.remove(thisSession)
        time.sleep(random.uniform(0.01, 0.2))
        

if __name__ == "__main__":
    main()
