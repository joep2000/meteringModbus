#!/usr/bin/env python3

from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta
from os import path
import sys
import os
import minimalmodbus
import time
import yaml
import logging
import json
import paho.mqtt.client as mqtt
from itertools import count
import time


# Change working dir to the same dir as this script
os.chdir(sys.path[0])

config = {
    'mqtt_host': os.environ.get('MQTT','192.168.2.2'),
    'mqtt_user': os.environ.get('MQTT_USER',None),
    'mqtt_pass': os.environ.get('MQTT_PASS',None),
    'mqtt_prefix': os.environ.get('MQTT_PREFIX','WP'),
    'mqtt_client': os.environ.get('MQTT_CLIENT','MBmeter'),
}

def mqtt_on_connect(client, userdata, flags, rc):
    log.info("MQTT connected!")
    client.subscribe("Verwarming/heating/State", 0)

def mqtt_on_message(client, userdata, msg):
    if msg.topic == "Verwarming/heating/State":
        if msg.payload == b'1':
            collector.Heating = True
        else:
            collector.Heating = False
    #if self.debug:
    log.info("RECIEVED MQTT MESSAGE: "+msg.topic + " " + str(msg.payload)+ " | heating= %s" % collector.Heating)

class DataCollector():
    def __init__(self, influx_client, meter_yaml, mqtt_client, instrument_client, prefix = config["mqtt_prefix"]):

        self.Heating = True
        self.influx_client = influx_client
        self.meter_yaml = meter_yaml
        self.max_iterations = None  # run indefinitely by default
        self.meter_map = None
        self.meter_map_last_change = -1

        self.mqtt_client = mqtt_client
        self.prefix = prefix

        self.Energy = 0
        self.TotalEnergy = 0
        self.PrevEnergy = 0
        self.HeatingEnergy = 0
        self.WaterEnergy = 0

        self.instrument = instrument_client
    

        log.info('Meters:')
        for meter in sorted(self.get_meters()):
            log.info('\t {} <--> {}'.format( meter['id'], meter['name']))

    def SendMeterEvent(self,power,energy,heating,water):
        topic = self.prefix+"/meterevent"
        msg = json.dumps({"power":power,"energy":energy,"heating":heating,"water":water})
        self.mqtt_client.publish(topic,msg,1)
        log.info('MQTT published energy: %.3f' % float(energy) )
        return


    def get_meters(self):
        assert path.exists(self.meter_yaml), 'Meter map not found: %s' % self.meter_yaml
        if path.getmtime(self.meter_yaml) != self.meter_map_last_change:
            try:
                log.info('Reloading meter map as file changed')
                new_map = yaml.load(open(self.meter_yaml), Loader=yaml.FullLoader)
                self.meter_map = new_map['meters']
                self.meter_map_last_change = path.getmtime(self.meter_yaml)
            except Exception as e:
                log.warning('Failed to re-load meter map, going on with the old one.')
                log.warning(e)
        return self.meter_map

    def collect_and_store(self):
        # self.instrument.debug = True
        meters = self.get_meters()
        t_utc = datetime.utcnow()
        t_str = t_utc.isoformat() + 'Z'

        datas = dict()
        meter_id_name = dict() # mapping id to name

        for meter in meters:
            meter_id_name[meter['id']] = meter['name']
            self.instrument.serial.baudrate = meter['baudrate']
            self.instrument.serial.bytesize = meter['bytesize']
            if meter['parity'] == 'none':
                self.instrument.serial.parity = minimalmodbus.serial.PARITY_NONE
            elif meter['parity'] == 'odd':
                self.instrument.serial.parity = minimalmodbus.serial.PARITY_ODD
            elif meter['parity'] == 'even':
                self.instrument.serial.parity = minimalmodbus.serial.PARITY_EVEN
            else:
                log.error('No parity specified')
                raise
            self.instrument.serial.stopbits = meter['stopbits']
            self.instrument.serial.timeout  = meter['timeout']    # seconds
            self.instrument.address = meter['id']    # this is the slave address number

            log.debug('Reading meter %s.' % (meter['id']))
            start_time = time.time()
            parameters = yaml.load(open(meter['type']), Loader=yaml.FullLoader)
            datas[meter['id']] = dict()

            for parameter in parameters:
                # If random readout errors occour, e.g. CRC check fail, test to uncomment the following row
                #time.sleep(0.01) # Sleep for 10 ms between each parameter read to avoid errors
                retries = 10
                while retries > 0:
                    try:
                        retries -= 1
                        if 'brand' in meter.keys() and meter['brand'] == 'orno':
                           log.debug("brand......%s",meter['brand'])
                           datas[meter['id']][parameter] = instrument.read_float(parameters[parameter], 3, 2, 0)
                        else:
                           datas[meter['id']][parameter] = instrument.read_float(parameters[parameter], 4, 2)
                          # log.debug('parameter: %s.' % datas[meter['id']][parameter])
                        retries = 0
                        pass
                    except ValueError as ve:
                        log.warning('Value Error while reading register {} from meter {}. Retries left {}.'
                               .format(parameters[parameter], meter['id'], retries))
                        log.error(ve)
                        if retries == 0:
                            raise RuntimeError
                    except TypeError as te:
                        log.warning('Type Error while reading register {} from meter {}. Retries left {}.'
                               .format(parameters[parameter], meter['id'], retries))
                        log.error(te)
                        if retries == 0:
                            raise RuntimeError
                    except IOError as ie:
                        log.warning('IO Error while reading register {} from meter {}. Retries left {}.'
                               .format(parameters[parameter], meter['id'], retries))
                        log.error(ie)
                        if retries == 0:
                            raise RuntimeError
                    except:
                        log.error("Unexpected error:", sys.exc_info()[0])
                        raise

            datas[meter['id']]['Read time'] =  time.time() - start_time
            log.debug("read time: %s",datas[meter['id']]['Read time'])

        json_body = [
            {
                'measurement': 'energy',
                'tags': {
                    'id': meter_id,
                    'meter': meter_id_name[meter_id],
                },
                'time': t_str,
                'fields': datas[meter_id]
            }
            for meter_id in datas
        ]
        if len(json_body) > 0:
            try:
                with self.influx_client.write_api() as write_api:
                    write_api.write("db_meters/autogen", "thuis", json_body)
                log.debug(t_str + ' Data written for %d meters.' % len(json_body))
                log.debug("heating: %s" % self.Heating)
                
                Power = datas[1]['Total system power']
                if self.PrevEnergy != 0:
                    self.Energy = (datas[1]['Import active energy'] - self.PrevEnergy) * 1000
                self.PrevEnergy = datas[1]['Import active energy']
                self.TotalEnergy += self.Energy

                if self.Heating == True:
                    self.HeatingEnergy += self.Energy
                if self.Heating == False:
                    self.WaterEnergy += self.Energy
                    # log.debug("heating is indeed %s" % self.Heating)
                self.SendMeterEvent(str(Power),str(self.TotalEnergy),str(self.HeatingEnergy),str(self.WaterEnergy))

            except Exception as e:
                log.error('Data not written!')
                log.error(e)
                raise
        else:
            log.warning(t_str, 'No data sent.')


def repeat(interval_sec, max_iter, func, *args, **kwargs):
    starttime = time.time()
    for i in count():
        if interval_sec > 0:
            time.sleep(interval_sec - ((time.time() - starttime) % interval_sec))
        if i % 1000 == 0:
            log.info('Collected %d readouts' % i)
        try:
            func(*args, **kwargs)
        except Exception as ex:
            log.error(ex)
        if max_iter and i >= max_iter:
            return


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', default=30,
                        help='Meter readout interval (seconds), default 30')
    parser.add_argument('--meters', default='meters.yml',
                        help='YAML file containing Meter ID, name, type etc. Default "meters.yml"')
    parser.add_argument('--log', default='CRITICAL',
                        help='Log levels, DEBUG, INFO, WARNING, ERROR or CRITICAL')
    parser.add_argument('--logfile', default='',
                        help='Specify log file, if not specified the log is streamed to console')
    args = parser.parse_args()
    interval = int(args.interval)
    loglevel = args.log.upper()
    logfile = args.logfile

    # Setup logging
    log = logging.getLogger('energy-logger')
    log.setLevel(getattr(logging, loglevel))

    if logfile:
        loghandle = logging.FileHandler(logfile, 'w')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        loghandle.setFormatter(formatter)
    else:
        loghandle = logging.StreamHandler()

    log.addHandler(loghandle)
    log.info('Started app')

    
    # Create the InfluxDB object
    influx_config = yaml.load(open('influx_config.yml'), Loader=yaml.FullLoader)
    client = InfluxDBClient(url=influx_config['host'],
                            token=influx_config['token'],
                            org=influx_config['org'])

    
    #Init and connect to MQTT server
    mqtt_client = mqtt.Client()
    mqtt_client.will_set(topic = "system/" + config["mqtt_prefix"], payload="Offline", qos=1, retain=True)
    user = config["mqtt_user"]
    password = config["mqtt_pass"]
    if user != None:
        mqtt_client.username_pw_set(user,password)
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_message = mqtt_on_message

    mqtt_client.connect(config["mqtt_host"],keepalive=10)
    mqtt_client.publish(topic = "system/"+ config["mqtt_prefix"], payload="Online", qos=1, retain=True)
    mqtt_client.loop_start()

    instrument = minimalmodbus.Instrument('/dev/ttyUSB1', 1) # port name, slave address (in decimal)
    instrument.mode = minimalmodbus.MODE_RTU   # rtu or ascii modei
    
    
    collector = DataCollector(influx_client=client,
                              meter_yaml=args.meters,mqtt_client=mqtt_client, instrument_client=instrument)


    try:
        repeat(interval,
           max_iter=collector.max_iterations,
           func=lambda: collector.collect_and_store())
    except Exception as e:
        log.error(e)
        instrument.serial.close()
        mqtt_client.loop_stop()
        client.close()
