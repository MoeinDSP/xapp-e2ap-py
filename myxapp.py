import src.e2ap_xapp as e2ap_xapp
import sys
sys.path.append("oai-oran-protolib/builds/")
from ran_messages_pb2 import *
from time import sleep
from ricxappframe.e2ap.asn1 import IndicationMsg
import csv 
import time

def metric_writer(ue_info):
    with open('ue_metrics.csv', 'a') as file:
        file_write = csv.writer(file)
        file_write.writerow(ue_info)

def xappLogic():

    # instanciate xapp 
    connector = e2ap_xapp.e2apXapp()

    # get gnbs connected to RIC
    gnb_id_list = connector.get_gnb_id_list()
    print("{} gNB connected to RIC, listing:".format(len(gnb_id_list)))
    for gnb_id in gnb_id_list:
        # subscription requests
        e2sm_buffer = e2sm_report_request_buffer()
        print(f"Subscription request sent to {gnb_id} ...")
    print("---------")

    with open('ue_metrics.csv', 'w') as file:
        file_write = csv.writer(file)
        file_write.writerow(['Timestamp', 'RNTI', 'RSRP', 'BER_UP', 'BER_DOWN', 'MCS_UP', 'MCS_DOWN', 'CELL_LOAD']) 

    while True:
        # Wait for 500 milliseconds
        sleep(0.5)
        received_messages = connector.get_queued_rx_message()

        num_received_msgs = len(received_messages)
        if num_received_msgs == 0:
            print("No messsage has been received yet!")
        else:
            print(f"{num_received_msgs} messages received!")
            for message in received_messages:
                if message["message type"] == connector.RIC_IND_RMR_ID:
                    # message decode
                    indm = IndicationMsg()
                    indm.decode(message["payload"])
                    ran_ind_resp = RAN_indication_response()
                    ran_ind_resp.ParseFromString(indm.indication_message)
                    
                    # UE metrics extraction
                    for params in ran_ind_resp.param_map:
                        if params.HasField('ue_list'):
                            ue_list = params.ue_list
                            for ue_info in ue_list.ue_info:
                                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                                rnti = ue_info.rnti
                                rsrp = ue_info.ue_rsrp
                                ber_up = ue_info.ue_ber_up if ue_info.HasField('ue_ber_up') else None
                                ber_down = ue_info.ue_ber_down if ue_info.HasField('ue_ber_down') else None
                                mcs_up = ue_info.ue_mcs_up if ue_info.HasField('ue_mcs_up') else None
                                mcs_down = ue_info.ue_mcs_down if ue_info.HasField('ue_mcs_down') else None
                                cell_load = ue_info.cell_load if ue_info.HasField('cell_load') else None
                                metric_writer([timestamp, rnti, rsrp, ber_up, ber_down, mcs_up, mcs_down, cell_load])
                                print(f"Received data added to database!")
        

def e2sm_report_request_buffer():
    master_mess = RAN_message()
    master_mess.msg_type = RAN_message_type.INDICATION_REQUEST
    inner_mess = RAN_indication_request()
    inner_mess.target_params.extend([RAN_parameter.GNB_ID, RAN_parameter.UE_LIST])
    master_mess.ran_indication_request.CopyFrom(inner_mess)
    buf = master_mess.SerializeToString()
    return buf

if __name__ == "__main__":
    xappLogic()