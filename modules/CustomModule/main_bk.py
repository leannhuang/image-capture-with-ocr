# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.

import time
import os
import sys
import asyncio
from six.moves import input
import threading
from azure.iot.device.aio import IoTHubModuleClient
import json
from datetime import datetime, timezone
from azure.iot.device import Message
import uuid


async def main():
    
    try:
        if not sys.version >= "3.5.3":
            raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
        print ( "IoT Hub Client for Python" )

        # The client object is used to interact with your Azure IoT hub.
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # connect the client.
        await module_client.connect()

        # define behavior for receiving an input message on input1
        async def input1_listener(module_client):
            while True:
                now = datetime.now()
                input_message = await module_client.receive_message_on_input("CountInput")  # blocking call
                print(f'{now} The data in the message received on azureeyemodule was {input_message.data}')
                print(f'{now} Custom properties are {input_message.custom_properties})')
                inference_list = json.loads(input_message.data)['NEURAL_NETWORK']
                print(f'inference list: {inference_list}')
                count_ppl = 0 
                utc_now = now

                if isinstance(inference_list, list) and inference_list:
                    utc_now = inference_list[0]['timestamp']
                    count_ppl = len(inference_list)
                
                print(f'count_ppl: {count_ppl}')

                json_data = {
                        'timestamp': utc_now,
                        'count': 1
                    }
                

                print("forwarding mesage to output1")
                msg = Message(json.dumps(json_data))
                msg.content_encoding = "utf-8"
                msg.content_type = "application/json"
                await module_client.send_message_to_output(msg, "output1")
        

        # define behavior for halting the application
        def stdin_listener():
            while True:
                try:
                    selection = input("Press Q to quit\n")
                    if selection == "Q" or selection == "q":
                        print("Quitting...")
                        break
                except:
                    time.sleep(10)

        # Schedule task for C2D Listener
        listeners = asyncio.gather(input1_listener(module_client))

        print ( "The sample is now waiting for messages. ")

        # Run the stdin listener in the event loop
        loop = asyncio.get_event_loop()
        user_finished = loop.run_in_executor(None, stdin_listener)

        # Wait for user to indicate they are done listening for messages
        await user_finished

        # Cancel listening
        listeners.cancel()

        # Finally, disconnect
        await module_client.disconnect()

    except Exception as e:
        print ( "Unexpected error %s " % e )
        raise

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

    # If using Python 3.7 or above, you can use following code instead:
    # asyncio.run(main())