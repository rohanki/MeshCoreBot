import asyncio
from meshcore import MeshCore
from meshcore.events import EventType
import yaml
import paho.mqtt.publish as publish
import json
import argparse
import random
import logging
import time
import csv
from meshcoredecoder import MeshCoreDecoder


#global variables
MY_NODES_ONLY = False
MY_NODES = ""
CONTACT_REFRESH_INTERVAL=120
GATE_TO_MQTT = True
MQTT_SERVER = ""
MQTT_PORT = ""
MQTT_USER = ""
MQTT_PASS = ""
MQTT_BASE_TOPIC = ""
PING_CHANNEL_NAMES = []
MY_LOCATION = ""
BANNED_PINGERS = []


last_decoded_packet = []

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#load settings from YAML
with open("conf.yaml", "r") as file:
    settings = yaml.safe_load(file)
MY_NODES_ONLY = settings.get("MY_NODES_ONLY")
CONTACT_REFRESH_INTERVAL = int(settings.get("CONTACT_REFRESH_INTERVAL"))
MQTT_SERVER = str(settings.get("MQTT_SERVER"))
MQTT_PORT = int(settings.get("MQTT_PORT"))
MQTT_USER = str(settings.get("MQTT_USER"))
MQTT_PASS = str(settings.get("MQTT_PASS"))
MQTT_BASE_TOPIC = str(settings.get("MQTT_BASE_TOPIC"))
GATE_TO_MQTT = bool(settings.get("GATE_TO_MQTT"))
MY_NODES = settings.get("MY_NODES")
PING_CHANNEL_NAMES = settings.get("PING_CHANNEL_NAMES")
MY_LOCATION = settings.get("MY_LOCATION")
BANNED_PINGERS = settings.get("BANNED_PINGERS")


#create a dict to store pingers
pingers={}


#load previous ping data
async def loadPingData():
    logger.info('Loading previous ping data from csv file')
    with open('pingdata.csv', mode='r', newline='', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        data_list = list(csv_reader)
    for pinger in data_list:
        #add ping data to pinger dict
        pingers[str(pinger[0])]  = {"count": int(pinger[1]), "lastping": int(time.time()), "inLast15": 0, "notified": False }

#handle incoming RX Log data. The RX log of the incoming packet should hit the RX log immediately before we get a message notification.
#this is used to get path data
async def handle_log_data(event):
    global last_decoded_packet
    packet = MeshCoreDecoder.decode(event.payload['payload'])
    last_decoded_packet = packet.to_dict()       
        


async def main():
    global Contacts, MQTT_BASE_TOPIC
    #get args
    argparser = argparse.ArgumentParser(description="MeshCoreBot for MeshCore!")
    argparser.add_argument("--port", type=str, help="Set the local Serial port")
    args = argparser.parse_args()
    if args.port:
        meshcoreport = args.port
    # Connect to device
    logger.info('🔌 Connecting to MeshCore Device on %s', meshcoreport)
    meshcore = await MeshCore.create_serial(meshcoreport)
    current_timestamp = int(time.time())
    await meshcore.commands.set_time(current_timestamp)
    logger.info('⏰ Clock Synced')
    await meshcore.start_auto_message_fetching()

    #load self info
    myNodeInfo = meshcore.self_info
    logger.info(myNodeInfo)
    #Update Base Topic
    MQTT_BASE_TOPIC += "/" + str(myNodeInfo['name']) + "/" + str(myNodeInfo['radio_freq']) + "/"

    #load previous ping data
    await loadPingData()





    # Set up event handlers
    
    async def handle_messages(event):
        global last_decoded_packet

        data = event.payload
        #Get contact details of sender from pub key prefix
        logger.info('📨Packet Received')
        logger.debug(data['text'])
        if str(data['type'])  =='PRIV':
            #this is a message is a DM
            sender = meshcore.get_contact_by_key_prefix(data['pubkey_prefix'])
            logger.info("[👤" + str(sender['adv_name']) + "] - "+ str(data['text']))
        else:
            #Channel Messages don't show senders pubkey or sender node name. Split sender name from message, then lookup sender in this nodes database.
            thisSender = meshcore.get_contact_by_name(str(data['text']).split(":",1)[0])
            #extract the actual text message
            thisMessage = str(data['text']).split(":",1)[1][1:]
            #check if a valid sender was returned, if not, add standard unknown data.
            if str(thisSender) == 'None':
                sender = {'adv_name': str(data['text']).split(":",1)[0],
                          'public_key': 'UNKNOWN'}
            else:
                #valid sender found, include this.
                sender = thisSender
            #get the channel name the incoming message was sent from. Only channel Index is present in the incoming message
            chanName = await meshcore.commands.get_channel(data['channel_idx'])
            #update channel name into data packet (for MQTT logging)
            data['channel_name'] = chanName.attributes['channel_name']
            logger.info("[👥 " + str(chanName.attributes['channel_name']) + "] - [👤" + str(sender['adv_name']) + "] - "+ str(data['text']))
            #Check to see what channel this came in on
            if str(chanName.attributes['channel_name']) in PING_CHANNEL_NAMES:
                logger.info('🏓 Incoming message on a Ping enabled channel')
                #message has come in on ping channel
                #check to see if pinger exists in dict already
                if str(sender['adv_name']) in pingers:
                    logger.info('Record for %s exists in database', str(sender['adv_name']))
                else:
                    pingers[str(sender['adv_name'])] = {"count": 0, "lastping": int(time.time()), "inLast15": 0, "notified": False }
                    logger.info('Record for %s does not yet exist in database. Adding', str(sender['adv_name']))
                #check to make sure message didn't come from Pingbot (as the word ping at the start of the message is the trigger)
                if str(sender['adv_name']) != 'PingBot':
                    logger.debug('🏓Message NOT from a pingbot')
                    #check the start of the text message is ping
                    if str(thisMessage.upper())[:4] == "PING":
                        logger.debug('🏓Valid ping command found')
                        #check sender
                        if str(sender['adv_name']) in BANNED_PINGERS: 
                                logger.info('🏓 Ping is from a banned user. Silently dropping request')
                        else:
                            #Valid ping command from a non banned pinger
                            #check to see how many pings in the last 15 mins
                            if int(pingers[str(sender['adv_name'])]['inLast15']) > 2:
                                logger.info('❌ %s has exceeded rate limit', str(sender['adv_name']))
                                #check to see if a rate limit notification has already been sent to this user. 
                                #to try and save bandwith (and stop users seeing the rate limit notificaiton in lieu of ping responds)
                                #the bot will only notify them of rate limiting once.
                                if pingers[str(sender['adv_name'])]['notified'] == False:
                                    logger.info('%s has not yet received rate limit notification. Sending', str(sender['adv_name']))
                                    repstring = '@[' + str(sender['adv_name']) + '] - You are now rate limited. Pings are limited to 3 every 15 mins'
                                    await meshcore.commands.send_chan_msg(int(data['channel_idx']), repstring)
                                    pingers[str(sender['adv_name'])]['notified'] = True
                            else:
                                logger.debug('%s is under the rate limiting threshold. Responding', str(sender['adv_name']))
                                #Update users values in the database.
                                pingers[str(sender['adv_name'])]['count']  = int(pingers[str(sender['adv_name'])]['count']) +1
                                pingers[str(sender['adv_name'])]['inLast15']  = int(pingers[str(sender['adv_name'])]['inLast15']) +1
                                pingers[str(sender['adv_name'])]['lastping'] = int(time.time())
                                #Construct the reply string. 
                                repstring = '@[' + str(sender['adv_name']) + '] Pong!🏓 Rx in ' + str(MY_LOCATION)
                                if int(data['path_len']) == 0:
                                    repstring += ' Directly.'
                                elif int(data['path_len']) == 1:
                                    repstring += ' via 1 hop.'
                                else:
                                    repstring += ' via ' + str(data['path_len']) + ' hops.'
                                    #check to see if we have hop data
                                    if 'path' in last_decoded_packet:
                                        logger.debug('Path info found. Appending')
                                        #append path and the first repeater
                                        repstring +=' (Path: ' + str(last_decoded_packet['path'][0])
                                        for rep_pre in last_decoded_packet['path'][1:]:
                                            #loop through repeaters starting at the second (position 1)
                                            repstring += '->' + str(rep_pre)
                                        repstring +=')'
                                repstring += '(seq:' + str(pingers[str(sender['adv_name'])]['count']) +')'                            

                                #check to see how many pings. Include warning on last
                                if int(pingers[str(sender['adv_name'])]['inLast15']) ==3:
                                    repstring += '. PS - you are now rate limited.' 
                                #wait between 200-500ms before replying
                                await asyncio.sleep((random.randint(2,5))/10)
                                #send the response
                                await meshcore.commands.send_chan_msg(int(data['channel_idx']), repstring)
                                logger.info('🏓 - ' + repstring)


        
        #Look for other commands. These will be DM'd back to user, but will fail if user is not in discovered node list.

        if '*test' in data['text']:
            #wait 300ms -let an ack happen before replying
            await asyncio.sleep(0.3)
            await meshcore.commands.send_msg(sender['public_key'], "ACK")
            logger.info('⚡ ACK sent')
        elif '*advert' in data['text']:
            #wait 300ms -let an ack happen before replying
            logger.info("👋Sending zero hop advert..")
            await asyncio.sleep(0.3)
            await meshcore.commands.send_advert(flood=False)
            await asyncio.sleep(3)
            await meshcore.commands.send_msg(sender['public_key'], "👋Sending zero hop advert..")

        elif '*flood' in data['text']:
            #wait 300ms -let an ack happen before replying
            logger.info("👋👋Sending flood advert..")
            await asyncio.sleep(0.3)
            await meshcore.commands.send_advert(flood=True)
            await asyncio.sleep(3)
            await meshcore.commands.send_msg(sender['public_key'], "👋👋Sending flood advert..")
        elif '*commands' in data['text']:
            await asyncio.sleep(0.3)
            await meshcore.commands.send_msg(sender['public_key'],
            '*commands - this\n'
            '*test - sends a simple ACK\n'
            '*flood - bot sends a flood advert\n'
            '*advert - bot sends a 0 hop advert\n'
            'ping - bot returns pong\n'
            )


   
        #lastly, add sender information to the event payload and publish to mqtt (if enabled)
        if GATE_TO_MQTT:
            #update sender info into data packet (for MQTT)
            data.update(sender)
            #if path data is available, add that to too
            if 'path' in last_decoded_packet:
                if str(last_decoded_packet['path']) !='None':
                    data['path'] = last_decoded_packet['path']
            #dump dict to json
            payload = json.dumps(data)
            publish.single(MQTT_BASE_TOPIC + "msg", payload, hostname=MQTT_SERVER, port=MQTT_PORT,auth={'username': MQTT_USER, 'password': MQTT_PASS})



    async def handle_advert(event):
        logger.debug('📖 Advert Received')
        thisAdverter = meshcore.get_contact_by_key_prefix(event.payload['public_key'])
        if str(thisAdverter) == 'None':
            #confirm a path has been received
            if 'out_path_len' in event:
                logger.info('📖Advert Received from: ' + str(event.payload['public_key']) + ' via ' + str(event['out_path_len']) + ' hops.')
            else:
                logger.info('📖Advert Received from: ' + str(event.payload['public_key']) + ' via an unknown path')
        else:
            if 'out_path_len' in thisAdverter:
                logger.info('📖Advert Received from: ' + str(thisAdverter['adv_name']) + ' via ' + str(thisAdverter['out_path_len']) + ' hops.')
            else:
                logger.info('📖Advert Received from: ' + str(thisAdverter['adv_name']) + ' via an unknown path')
        #Gate to MQTT (if enabled)
        if GATE_TO_MQTT:
            #add lookup and add contact details to payload
            try:
                event.payload.update(meshcore.get_contact_by_key_prefix(event.payload['public_key']))
            finally:
                payload = json.dumps(event.payload)
                publish.single(MQTT_BASE_TOPIC + "advert", payload, hostname=MQTT_SERVER, port=MQTT_PORT,auth={'username': MQTT_USER, 'password': MQTT_PASS})

    
    
    # Subscribe to events
    meshcore.subscribe(EventType.CONTACT_MSG_RECV, handle_messages)
    meshcore.subscribe(EventType.CHANNEL_MSG_RECV, handle_messages)
    meshcore.subscribe(EventType.ADVERTISEMENT, handle_advert)
    meshcore.subscribe(EventType.RX_LOG_DATA,handle_log_data)



    #Functions
    async def refresh_contacts():
        #refresh contacts
        while True:
            result = await meshcore.commands.get_contacts()
            if result.type == EventType.ERROR:
                print(f"Error getting contacts: {result.payload}")
            else:
                Contacts = result.payload
            await asyncio.sleep(CONTACT_REFRESH_INTERVAL)  # Wait before refreshing


    async def decrementPingers():
            while True:
                #wait 5 mins before doing anything
                await asyncio.sleep(300)
                logger.info('Starting pinger decrementation')
                #setup list to write pinger and count to a CSV
                pingdata = []
                for pinger, pingervals in pingers.items():
                    #add pinger to list to write out
                    pingdata.append({'name': str(pinger), "total": str(pingervals['count'])})
                    #print(f"Pinger: {pinger}, Values: {pingervals}")
                    if int(pingervals['inLast15']) > 0:
                        pingervals['inLast15'] = int(pingervals['inLast15']) -1
                if len(pingdata)>0:
                    logger.info('starting file write')
                    with open ('pingdata.csv','w',newline='', encoding='utf-8') as file:
                        writer = csv.DictWriter(file,fieldnames=["name", "total"])
                        writer.writerows(pingdata)


        # Start background refresh tasks
    contact_update_task = asyncio.create_task(refresh_contacts())
    pinger_update_tast = asyncio.create_task(decrementPingers())
    
    try:
        # Keep the main program running
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        # Clean up when program ends
        await meshcore.disconnect()
        contact_update_task.cancel()

# Run the program
asyncio.run(main())

