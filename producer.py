import pandas as pd
from sodapy import Socrata
from datetime import datetime
import time
import os
import json

import configparser
config = configparser.ConfigParser()
#config.read('kafka_config_local.conf')
config.read('kafka_config.conf')

# Kafka imports
from kafka import KafkaProducer
# definition du serializer en json
#producer = KafkaProducer(bootstrap_servers=config['DEFAULT']['bootstrap.servers'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers=config['DEFAULT']['bootstrap.servers'], 
                        security_protocol=config['DEFAULT']['security.protocol'], 
                        sasl_mechanism = config['DEFAULT']['sasl.mechanisms'], 
                        sasl_plain_username = config['DEFAULT']['sasl.username'], 
                        sasl_plain_password  = config['DEFAULT']['sasl.password'], 
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                        connections_max_idle_ms=3600000)

import logging
logging.basicConfig(handlers=[logging.FileHandler(filename="logapp/logapp_{0}.log".format(str(datetime.now().date())), 
                                                 encoding='utf-8', mode='a+')], level=logging.INFO, force=True)

logging.info(str(datetime.now()) + " - Création du client Socrata API")
client = Socrata(config['SOCRATA']['api_url'], config['SOCRATA']['api_key'])

while True : 
    logging.info(str(datetime.now()) + " - Début de la boucle infini")
    logging.basicConfig(handlers=[logging.FileHandler(filename="logapp/logapp_{0}.log".format(str(datetime.now().date())), 
                                                 encoding='utf-8', mode='a+')], level=logging.INFO, force=True)
    # if the file is empty we don't try to read it
    if os.stat("last_value_date").st_size == 0 :
        logging.info(str(datetime.now()) + " - La last_value_date n'existe pas on requete toutes les données")
        # First 20000 results, returned as JSON from API / converted to Python list of
        # dictionaries by sodapy.
        results = client.get(config['SOCRATA']['dataset_id'], limit=20000, order="_last_updt DESC")
        
    # else we read only the last line of it in order to not load all the file in the memory
    else :
        logging.info(str(datetime.now()) + " - la last_value_date existe on va la récupérer")
        with open("last_value_date", "r") as f:
            for line in f: pass
            last_value = line
            logging.info(str(datetime.now()) + " - last_value_date récupérée : {0}".format(last_value))
        
        results = client.get(config['SOCRATA']['dataset_id'], limit=200000, order="_last_updt DESC", where="_last_updt > '{0}'".format(last_value))
    
    results_df = pd.DataFrame.from_records(results)
    
    nb_new_data = results_df.shape[0]
    # si on a des nouvelles données à traiter
    
    if nb_new_data>0 :
        logging.info(str(datetime.now()) + " - On a {0} nouvelles lignes de données à traiter".format(nb_new_data))
        # Convert to pandas DataFrame
        
        max_date_delta = max(results_df["_last_updt"])
        logging.info(str(datetime.now()) + " - nouvelle last_value_date à insérer : {0}".format(max_date_delta))
        
            
        df_to_json = json.loads(results_df.to_json(orient='records', force_ascii=False))
        
        for record in df_to_json :
            logging.info(str(datetime.now()) + " - Envoie du message suivant dans le topic {0} : {1}".format(config['DEFAULT']['topic_name'], record))
            producer.send(config['DEFAULT']['topic_name'], record)
        producer.flush()
        
        with open("last_value_date", "a") as f:
            f.write("\n{0}".format(max_date_delta))
    else : 
        logging.info(str(datetime.now()) + " - Pas de nouvelles lignes de données à traiter")
    # Block until all pending messages are at least put on the network
    
            
    logging.info(str(datetime.now()) + " - Début du sleep de 5 min")
    time.sleep(300)
    logging.info(str(datetime.now()) + " - Sortie du sleep")
    
