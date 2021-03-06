import yaml
import json
import argparse, sys, logging
from confluent_kafka import Producer

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
    # sys.exit(0)
    selector = args.mode

    if args.dry_run:
        producer = None
        topic = None
    else:
        clickTopic = config['General']['topic']
        logging.debug(f'topic: {topic}')
        kafkaconf = config['Kafka']
        kafkaconf['client.id'] = socket.gethostname()
        logging.debug(f'Kafka client configuration: {kafkaconf}')
        producer = Producer(kafkaconf)

    # Do something here

if __name__ == "__main__":
    main()
