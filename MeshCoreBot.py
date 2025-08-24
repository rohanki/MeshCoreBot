import asyncio
from meshcore import MeshCore, EventType
import yaml
import paho.mqtt.publish as publish
import json
import argparse


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
GET_REPETAER_STATS = False
REPEATER_CHECK_INTERVAL = ""
REPEATERS = ""
REPEATER_PASSWORDS = ""

Contacts=[]


#load settings
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
GET_REPETAER_STATS = bool(settings.get("GET_REPETAER_STATS"))
REPEATER_CHECK_INTERVAL = int(settings.get("REPEATER_CHECK_INTERVAL"))
REPEATERS = settings.get("REPEATERS")
REPEATER_PASSWORDS = settings.get("REPEATER_PASSWORDS")
MY_NODES = settings.get("MY_NODES")

print(MQTT_SERVER)

async def get_repeater_name(mc, hash_prefix):
    """Find a contact by its 2-character hash prefix and return its name"""
    # Ensure contacts are available
    await mc.ensure_contacts()
    
    # Find contact with matching hash prefix
    contact = mc.get_contact_by_key_prefix(hash_prefix)
    if contact:
        return contact.get("adv_name", f"Unknown ({hash_prefix})")
    else:
        return f"Unknown ({hash_prefix})"






async def main():
    global Contacts, MQTT_BASE_TOPIC
    #get args
    argparser = argparse.ArgumentParser(description="MeshCoreBot for MeshCore!")
    argparser.add_argument("--port", type=str, help="Set the local Serial port")
    args = argparser.parse_args()
    if args.port:
        meshcoreport = args.port

    # Connect to device
    meshcore = await MeshCore.create_serial(meshcoreport)
    await meshcore.start_auto_message_fetching()

    #load self info
    myNodeInfo = meshcore.self_info
    print(myNodeInfo)
    #Update Base Topic
    MQTT_BASE_TOPIC += "/" + str(myNodeInfo['name']) + "/" + str(myNodeInfo['radio_freq']) + "/"

    
    # Set up event handlers
    async def handle_ack(event):
        print("Message acknowledged!")
        print(event.payload)
        publish.single(MQTT_BASE_TOPIC + "ack", str(event.payload), hostname=MQTT_SERVER, port=MQTT_PORT,auth={'username': MQTT_USER, 'password': MQTT_PASS})
    
    async def handle_battery(event):
        print(f"Battery level: {event.payload}%")
    
    async def handle_advert(event):
        print("Advertisement Detected")
        print(event.payload)
        #Gate to MQTT (if enabled)
        if GATE_TO_MQTT:
            print(meshcore.get_contact_by_key_prefix(event.payload['public_key']))
            #add lookup and add contact details to payload
            event.payload.update(meshcore.get_contact_by_key_prefix(event.payload['public_key']))
            publish.single(MQTT_BASE_TOPIC + "advert", str(event.payload), hostname=MQTT_SERVER, port=MQTT_PORT,auth={'username': MQTT_USER, 'password': MQTT_PASS})



    async def handle_messages(event):
        data = event.payload
        print('Event:')
        print(event)
        print(data['text'])
        print(data)

        #Get contact details of sender from pub key prefix
        if str(data['type'])  =='PRIV':
            #this is a message is a DM
            sender = meshcore.get_contact_by_key_prefix(data['pubkey_prefix'])
            print('Received incoming DM from: ' + str(sender['adv_name']))
        else:
            #Channel Messages don't show senders pubkey. Attempt to lookup by name
            thisSender = meshcore.get_contact_by_name(str(data['text']).split(":",1)[0])
            print(thisSender)
            if str(thisSender) == 'None':
                sender = {}
            else:
                sender = thisSender
            print('Received group message from: ' + str(sender['adv_name']))
        #Look for commands
        if '#test' in data['text']:
            #wait 300ms -let an ack happen before replying
            await asyncio.sleep(0.3)
            await meshcore.commands.send_msg(sender['public_key'], "ACK")
            print('Test Message Acknowledged')
        elif '#advert' in data['text']:
            #wait 300ms -let an ack happen before replying
            await asyncio.sleep(0.3)
            await meshcore.commands.send_advert(flood=True)
            await asyncio.sleep(3)
            await meshcore.commands.send_msg(sender['public_key'], "Sending zero hop advert..")

        elif '#myroute' in data['text']:
            #work out our outbound route to the sender
            print('my route')
            if 'out_path_len' in sender:
                if int(sender['out_path_len']) == 0:
                    routeText = 'Receiving you directly'
                else:
                    routeText = 'Transmitting to you via ' + str(sender['out_path_len']) + ' hop(s):\n\n'
                    routeList = [(str(sender['out_path'])[i:i+2]) for i in range(0, len(str(sender['out_path'])), 2)]
                    print(routeList)
                    for repeater in routeList:
                        print(meshcore.get_contact_by_key_prefix(repeater))
                        routeText += str(meshcore.get_contact_by_key_prefix(repeater)['adv_name']) + '\n->\n'
                    routeText +='You'
            else:
                    routeText = 'Unable to determine route to you'
            #wait 300ms -let an ack happen before replying
            await asyncio.sleep(0.3)
            await meshcore.commands.send_msg(sender['public_key'], routeText)
            print(routeText)
        elif '#commands' in data['text']:
            await asyncio.sleep(0.3)
            await meshcore.commands.send_msg(sender['public_key'],
            '#commands - this\n'
            '#test - sends a simple ACK\n'
            '#advert - bot sends an advert\n'
            '#myroute - show bots route to you'
            )



        #lastly, add sender information to the event payload and publish to mqtt (if enabled)
        if GATE_TO_MQTT:
            data.update(sender)
            publish.single(MQTT_BASE_TOPIC + "msg", str(event.payload), hostname=MQTT_SERVER, port=MQTT_PORT,auth={'username': MQTT_USER, 'password': MQTT_PASS})


    def handle_rf_packet(event):
        packet = event.payload
        if isinstance(packet, dict):
            print(f"Raw RF packet received:")
            if 'snr' in packet:
                print(f"  SNR: {packet['snr']:.1f} dB")
            if 'rssi' in packet:
                print(f"  RSSI: {packet['rssi']} dBm")
            if 'payload_length' in packet:
                print(f"  Payload length: {packet['payload_length']} bytes")
            if 'payload' in packet:
                print(f"  Payload (hex): {packet['payload']}")
        else:
            print(f"RF packet received: {packet}")
    
    
    # Subscribe to events
    meshcore.subscribe(EventType.ACK, handle_ack)
 #   meshcore.subscribe(EventType.RX_LOG_DATA, handle_rf_packet)
    meshcore.subscribe(EventType.BATTERY, handle_battery)
    meshcore.subscribe(EventType.CONTACT_MSG_RECV, handle_messages)
    meshcore.subscribe(EventType.ADVERTISEMENT,handle_advert)
    meshcore.subscribe(EventType.CHANNEL_MSG_RECV, handle_messages)


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


    async def get_repeater_stats():
        while True:
            print('getting repeater stats')
            for i in range(len(REPEATERS)):
                print(i)
                login_event = await meshcore.commands.send_login(REPEATERS[i], REPEATER_PASSWORDS[i])
                if login_event.type != EventType.ERROR:
                    print('getting status')
                    await meshcore.commands.send_statusreq(REPEATERS[i])
                    status_event = await meshcore.wait_for_event(EventType.STATUS_RESPONSE, timeout=7)
                    if status_event:
                        print(status_event.payload)


            await asyncio.sleep(REPEATER_CHECK_INTERVAL)  # Wait before refreshing

        # Start background refresh tasks
    contact_update_task = asyncio.create_task(refresh_contacts())
    if GET_REPETAER_STATS:
        repeater_update_task = asyncio.create_task(get_repeater_stats())
   


    try:
        # Keep the main program running
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        # Clean up when program ends
        await meshcore.disconnect()
        contact_update_task.cancel()

# Run the program
asyncio.run(main())

