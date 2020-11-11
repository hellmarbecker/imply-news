import yaml
import json
import random
import time
import argparse, sys, logging
import socket
from faker import Faker
from confluent_kafka import Producer
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

# Session model

class Session:
        
    def __init__(self, states, initialState, stateTransitionMatrix, **kwargs):
        if initialState not in states:
            raise InvalidStateException()
        self.startTime = None # this is set upon the first advance() to match the first eventTime
        self.states = states
        self.state = initialState
        self.statesVisited = { initialState }
        self.stateTransitionMatrix = stateTransitionMatrix
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return "{}({!r})".format(type(self).__name__, self.__dict__)

    def advance(self):
        newState = selectAttr(self.stateTransitionMatrix[self.state])
        if newState is None:
            raise InvalidTransitionException()
        self.eventTime = time.time()
        if self.startTime is None:
            self.startTime = self.eventTime
        logging.debug(f'advance(): from {self.state} to {newState}')
        self.state = newState
        self.statesVisited.add(newState)

    def url(self):
        return baseurl + '/' + self.state + '/' + self.contentId + '/' + self.subContentId

# Output functions - write to Kafka, or to stdout as JSON

def emit(p, t, e):
    sid = e['sid']
    if p is None:
        print(f'{sid}|{json.dumps(e)}')
    else:
        p.produce(t, key=str(sid), value=json.dumps(e))
        p.poll(0)

def emitClick(p, t, s):
    emitRecord = {
        'timestamp' : time.time(),
        'recordType' : 'click',
        'url' : s.url(),
        'state' : s.state,
        'statesVisited' : str(s.statesVisited),
        'sid' : s.sid,
        'campaign' : s.campaign,
        'channel' : s.channel,
        'contentId' : s.contentId,
        'subContentId' : s.subContentId,
        'gender' : s.gender,
        'age' : s.age
    }
    emit(p, t, emitRecord)

def emitSession(p, t, s):
    emitRecord = {
        'timestamp' : s.startTime,
        'recordType' : 'session',
        'sid' : s.sid,
        'campaign' : s.campaign,
        'channel' : s.channel,
        'gender' : s.gender,
        'age' : s.age
    }
    # explode and pivot the states visited
    emitRecord.update( { t : (t in s.statesVisited) for t in s.states } )
    emit(p, t, emitRecord)

# Read configuration

def readConfig(ifn):
    logging.debug(f'reading config file {ifn}')
    with open(ifn, 'r') as f:
        c = yaml.load(f, Loader=yaml.FullLoader)
        logging.debug(f'Configuration: {c}')
        return c


# --- Main entry point ---

def main():

    logLevel = logging.INFO
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', help='Enable debug logging', action='store_true')
    parser.add_argument('-q', '--quiet', help='Quiet mode (overrides Debug mode)', action='store_true')
    parser.add_argument('-f', '--config', help='Configuration file for session state machine(s)', required=True)
    parser.add_argument('-m', '--mode', help='Mode for session state machine(s)', default='default')
    parser.add_argument('-n', '--dry-run', help='Write to stdout instead of Kafka',  action='store_true')
    args = parser.parse_args()

    if args.debug:
        logLevel = logging.DEBUG
    if args.quiet:
        logLevel = logging.ERROR

    logging.basicConfig(level=logLevel)

    cfgfile = args.config
    config = readConfig(cfgfile)
    # sys.exit(0)
    selector = args.mode

    if args.dry_run:
        producer = None
        clickTopic = None
        sessionTopic = None
    else:
        brokers = config['Kafka']['brokers']
        clickTopic = config['Kafka']['clickTopic']
        sessionTopic = config['Kafka']['sessionTopic']
        kafkaconf = {'bootstrap.servers': brokers,'client.id': socket.gethostname()}
        logging.debug(f'brokers: {brokers} clickTopic: {clickTopic} sessionTopic: {sessionTopic}')
        producer = Producer(kafkaconf)

    sessionId = 0
    allSessions = []

    while True:
        logging.debug('Top of loop')
        logging.debug(f'Total elements in list: {len(allSessions)}')
        logging.debug(f'state selector: {selector}')
        # With a certain probability, create a new session
        if random.random() < 0.5:
            sessionId += 1
            logging.debug(f'--> Creating Session: id {sessionId}')
            salesAmount = random.uniform(10.0, 90.0);
            States = config['StateMachine']['States']
            StateTransitionMatrix = config['StateMachine']['StateTransitionMatrix'][selector]
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
            emitClick(producer, clickTopic, newSession)
            if not args.quiet:
                sys.stderr.write('.')
                sys.stderr.flush()
            allSessions.append(newSession)
        # Pick one of the sessions
        try:
            thisSession = random.choice(allSessions)
            logging.debug(f'--> Session id {thisSession.sid}')
            logging.debug(thisSession)
            thisSession.advance()
            emitClick(producer, clickTopic, thisSession)
            if not args.quiet:
                sys.stderr.write('.')
                sys.stderr.flush()
        except IndexError:
            logging.debug('--> No sessions to choose from')
        except KeyError:
            emitSession(producer, sessionTopic, thisSession)
            if not args.quiet:
                sys.stderr.write(':')
                sys.stderr.flush()
            # Here we end up when the session was in exit state
            logging.debug(f'--> removing session id {thisSession.sid}')
            allSessions.remove(thisSession)
        time.sleep(random.uniform(0.001, 0.02))
        

if __name__ == "__main__":
    main()
