import src.e2ap_xapp as e2ap_xapp
import sys
sys.path.append("oai-oran-protolib/builds/")
from ran_messages_pb2 import *
from time import sleep
from ricxappframe.e2ap.asn1 import IndicationMsg
import csv 
import time


def xappLogic():

    # instanciate xapp 
    connector = e2ap_xapp.e2apXapp()

    # get gnbs connected to RIC
    gnb_id_list = connector.get_gnb_id_list()
    print("{} gNB connected to RIC, listing:".format(len(gnb_id_list)))
    for gnb_id in gnb_id_list:
        print(gnb_id)
    print("---------")

    with open('ue_metrics.csv', 'w') as file:
        file_write = csv.writer(file)
        file_write.writerow(['Timestamp', 'RNTI', 'RSRP', 'BER_UP', 'BER_DOWN', 'MCS_UP', 'MCS_DOWN', 'CELL_LOAD']) 

    while True:
        # subscription requests
        for gnb in gnb_id_list:
            e2sm_buffer = e2sm_report_request_buffer()
            # sending indication request
            connector._rmr_send_w_meid(e2sm_buffer, connector.RIC_IND_RMR_ID, bytes(gnb, 'ascii'))
            # connector.send_e2ap_sub_request(e2sm_buffer,gnb)
            # connector.send_e2ap_control_request(e2sm_buffer,gnb)

            # receiving indication response
            received_message = None
            start_time = time.time()
            while time.time() - start_time < 2 and received_message is None:
                messages = connector.get_queued_rx_message()
                for message in messages:
                    if message["message type"] == connector.RIC_IND_RMR_ID:
                        received_message = message  
                sleep(0.2)

            if received_message is None:
                continue
                
            # message decode
            indm = IndicationMsg()
            indm.decode(received_message["payload"])
            ran_ind_resp = RAN_indication_response()
            ran_ind_resp.ParseFromString(indm.indication_message)
            
            # UE metrics extraction
            ue_metrics = []
            ue_list = None
            for i in ran_ind_resp.param_map:
                if i.HasField('ue_list'):
                    ue_list = i.ue_list

                if ue_list is not None:
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    for ue_info in ue_list.ue_info:
                        rnti = ue_info.rnti
                        rsrp = ue_info.ue_rsrp
                        ber_up = ue_info.ue_ber_up if ue_info.HasField('ue_ber_up') else None
                        ber_down = ue_info.ue_ber_down if ue_info.HasField('ue_ber_down') else None
                        mcs_up = ue_info.ue_mcs_up if ue_info.HasField('ue_mcs_up') else None
                        mcs_down = ue_info.ue_mcs_down if ue_info.HasField('ue_mcs_down') else None
                        cell_load = ue_info.cell_load if ue_info.HasField('cell_load') else None

                    ue_metrics.append([timestamp, rnti, rsrp, ber_up, ber_down, mcs_up, mcs_down, cell_load])
            
            # save
            with open('ue_metrics.csv', 'a') as file:
                file_write = csv.writer(file)
                for ue_info in ue_metrics:
                    file_write.writerow(ue_info)

        sleep(0.5) # Wait for 500 milliseconds

    # # read loop
    # sleep_time = 4
    # while True:
    #     print("Sleeping {}s...".format(sleep_time))
    #     sleep(sleep_time)
    #     messgs = connector.get_queued_rx_message()
    #     if len(messgs) == 0:
    #         print("{} messages received while waiting".format(len(messgs)))
    #         print("____")
    #     else:
    #         print("{} messages received while waiting, printing:".format(len(messgs)))
    #         for msg in messgs:
    #             if msg["message type"] == connector.RIC_IND_RMR_ID:
    #                 print("RIC Indication received from gNB {}, decoding E2SM payload".format(msg["meid"]))
    #                 indm = IndicationMsg()
    #                 indm.decode(msg["payload"])
    #                 resp = RAN_indication_response()
    #                 resp.ParseFromString(indm.indication_message)
    #                 print(resp)
    #                 print("___")
    #             else:
    #                 print("Unrecognized E2AP message received from gNB {}".format(msg["meid"]))

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