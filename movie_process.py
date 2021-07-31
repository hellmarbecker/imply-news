import yaml
import json
import random
import time
import argparse, sys, logging
import socket
from faker import Faker
from confluent_kafka import Producer
from exceptions import InvalidStateException, InvalidTransitionException

fake = Faker()

# Dimension values

d_movies = [ 'Alien', 'High Noon', 'Black Widow', 'Aladdin', '50 First Dates' ]
d_tags = [ 'murder', 'mystery', 'horror', 'sci-fi', 'romantic', 'comedy', 'kids', 'cartoon', 'superhero', 'western' ]
 

msgCount = 0

# Output functions - write to Kafka, or to stdout as JSON

def emit(producer, topic, emitRecord):
    global msgCount
    sid = emitRecord['sid']
    if producer is None:
        print(f'{sid}|{json.dumps(emitRecord)}')
    else:
        producer.produce(topic, key=str(sid), value=json.dumps(emitRecord))
        msgCount += 1
        if msgCount >= 2000:
            producer.flush()
            msgCount = 0
        producer.poll(0)

def emitClick(p, t, s):
    emitRecord = {
        'timestamp' : time.time(),
        'recordType' : 'click',
        'url' : s.url(),
        'useragent' : s.useragent,
        'statuscode' : selectAttr(d_statuscode),
        'state' : s.state,
        'statesVisited' : str(s.statesVisited),
        'sid' : s.sid,
        'uid' : s.uid,
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
        'useragent' : s.useragent,
        'sid' : s.sid,
        'uid' : s.uid,
        'campaign' : s.campaign,
        'channel' : s.channel,
        'gender' : s.gender,
        'age' : s.age
    }
    # explode and pivot the states visited
    emitRecord.update( { t : (t in s.statesVisited) for t in s.states } )
    emit(p, t, emitRecord)

# Check configuration

def checkConfig(cfg):
    eps = 0.0001
    states = set(cfg['StateMachine']['States'])
    modes = cfg['StateMachine']['StateTransitionMatrix']
    logging.debug(f"Transition Matrix: {modes}")
    logging.debug(f"Transition Matrix: {modes.items()}")
    for mode in modes.keys():
        for originState, transitions in modes[mode].items():
            # is the origin state in the list?
            if originState not in states:
                logging.debug(f'Mode {mode}: originState {originState} does not exist')
                raise Exception
            # do the target states match the main states list?
            if set(transitions.keys()) != states:
                logging.debug(f'Mode {mode} originState {originState}: transitions do not match state list')
                raise Exception
            # are the probabilities okay?
            if abs(sum(transitions.values()) - 1.0) > eps:
                logging.debug(f'Mode {mode} originState {originState}: transition probabilities do not add up to 1')
                raise Exception

# Read configuration

def readConfig(ifn):
    logging.debug(f'reading config file {ifn}')
    with open(ifn, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
        # get include files if present
        for inc in cfg.get("IncludeOptional", []):
            try:
                logging.debug(f'reading include file {inc}')
                cfg.update(yaml.load(open(inc), Loader=yaml.FullLoader))
            except FileNotFoundError:
                logging.debug(f'optional include file {inc} not found, continuing')
        logging.debug(f'Configuration: {cfg}')
        checkConfig(cfg)
        return cfg


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

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logLevel)

    cfgfile = args.config
    config = readConfig(cfgfile)
    # sys.exit(0)
    selector = args.mode

    if args.dry_run:
        producer = None
        clickTopic = None
        sessionTopic = None
    else:
        clickTopic = config['General']['clickTopic']
        sessionTopic = config['General']['sessionTopic']
        logging.debug(f'clickTopic: {clickTopic} sessionTopic: {sessionTopic}')
        kafkaconf = config['Kafka']
        kafkaconf['client.id'] = socket.gethostname()
        logging.debug(f'Kafka client configuration: {kafkaconf}')
        producer = Producer(kafkaconf)

    # start timestamp

    timeStart = time.mktime(time.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'))
    n = 0
    timeX = timeStart
    while True:
        if n % 10 == 0:
            thatMovie = fake.random_element(d_movies) 
            thoseTags = fake.random_elements(elements=d_tags, unique=False) # Allow repetition so as to test multivalue dimensions
            #emitMovie(...)
        thisMovie = fake.random_element(d_movies)

        print(f'{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timeX))},{thatMovie},{thisMovie},{"|".join(thoseTags)}')
        time.sleep(0.1)
        timeX += 60


        

if __name__ == "__main__":
    main()
