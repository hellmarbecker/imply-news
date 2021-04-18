import yaml
import json
import random
import time
import datetime
import argparse, sys, logging
import socket
from faker import Faker
from confluent_kafka import Producer
from exceptions import InvalidStateException, InvalidTransitionException

key_field = 'uid'
fake = Faker()

# Output functions - write to Kafka, or to stdout as JSON

# Write a record with key
def emit(producer, topic, emitRecord):
    kkey = emitRecord[key_field]
    if producer is None:
        print(f'{kkey}|{json.dumps(emitRecord)}')
    else:
        producer.produce(topic, key=str(kkey), value=json.dumps(emitRecord))
        producer.poll(0)

def emitFact(p, t, k):
    ts = datetime.datetime.now().replace(microsecond=0).isoformat()
    emitRecord = {
        key_field : k,
        'timeIngest' : tsIngest,
        'timeEvent' : tsEvent,
        'measure1' : 
    }
    emit(p, t, emitRecord)

def emitDimChange(p, t, k):
    ts = datetime.datetime.now().replace(microsecond=0).isoformat()
    emitRecord = {
        key_field : k,
        'timeIngest' : tsIngest,
        'timeEvent' : tsEvent,
        'dimValue' : fake.city(20)
    }
    # explode and pivot the states visited
    emitRecord.update( { t : (t in s.statesVisited) for t in s.states } )
    emit(p, t, emitRecord)

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
        return cfg


# --- Main entry point ---

def main():

    Faker.seed(0)

    logLevel = logging.INFO
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', help='Enable debug logging', action='store_true')
    parser.add_argument('-q', '--quiet', help='Quiet mode (overrides Debug mode)', action='store_true')
    parser.add_argument('-f', '--config', help='Configuration file', required=True)
    parser.add_argument('-n', '--dry-run', help='Write to stdout instead of Kafka',  action='store_true')
    args = parser.parse_args()

    if args.debug:
        logLevel = logging.DEBUG
    if args.quiet:
        logLevel = logging.ERROR

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logLevel)

    cfgfile = args.config
    config = readConfig(cfgfile)

    if args.dry_run:
        producer = None
        factTopic = None
        dimTopic = None
    else:
        factTopic = config['General']['factTopic']
        dimTopic = config['General']['dimTopic']
        logging.debug(f'factTopic: {factTopic} dimTopic: {dimTopic}')
        kafkaconf = config['Kafka']
        kafkaconf['client.id'] = socket.gethostname()
        logging.debug(f'Kafka client configuration: {kafkaconf}')
        producer = Producer(kafkaconf)

    while True:
        logging.debug('Top of loop')
        logging.debug(f'Total elements in list: {len(allSessions)}')

        emitClick(producer, factTopic, newSession)
        if not args.quiet:
            sys.stderr.write('.')
            sys.stderr.flush()
        time.sleep(random.uniform(0.001, 0.02))
        

if __name__ == "__main__":
    main()
