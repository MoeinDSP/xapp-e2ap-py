import src.e2ap_xapp as e2ap_xapp
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

    # subscription requests
    for gnb in gnb_id_list:
        e2sm_buffer = e2sm_report_request_buffer()
        connector.send_e2ap_sub_request(e2sm_buffer,gnb)
        #connector.send_e2ap_control_request(e2sm_buffer,gnb)
    
        with open('data.csv', 'a') as file:
            file_write = csv.writer(file)
            file_write.writerow(['Timestamp', 'RSRP', 'BER_UP', 'BER_DOWN', 'MCS_UP', 'MCS_DOWN', 'CELL_LOAD'])
            try:
                while True:
                    msg = connector.get_queued_rx_message()[0]
                    indm = IndicationMsg()
                    indm.decode(msg["payload"])
                    ran_ind_resp = RAN_indication_response()
                    ran_ind_resp.ParseFromString(indm.indication_message)
                    print(ran_ind_resp)

                    ue_list = None  

                    for i in ran_ind_resp.param_map:
                        if i.HasField('ue_list'):
                            ue_list = i.ue_list

                        if ue_list is not None:
                            prbs = ue_list.allocated_prbs
                            rsrp_avg = 0
                            ber_up_avg = 0
                            ber_down_avg = 0
                            mcs_up_avg = 0
                            mcs_down_avg = 0
                            cell_load_avg = 0

                            for ue_info in ue_list.ue_info:
                                rsrp_avg += ue_info.ue_rsrp
                                ber_up_avg += ue_info.ue_ber_up
                                ber_down_avg += ue_info.ue_ber_down
                                mcs_up_avg += ue_info.ue_mcs_up
                                mcs_down_avg += ue_info.ue_mcs_down
                                cell_load_avg += ue_info.cell_load

                            rsrp_avg /= prbs
                            ber_up_avg /= prbs
                            ber_down_avg /= prbs
                            mcs_up_avg /= prbs
                            mcs_down_avg /= prbs
                            cell_load_avg /= prbs

                            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                            data = [timestamp, rsrp_avg, ber_up_avg, ber_down_avg, mcs_up_avg, mcs_down_avg, cell_load_avg]
                            file_write.writerow(data)
                    
                    print(ran_ind_resp)
                    sleep(0.5) # Wait for 500 milliseconds
                    connector.send_e2ap_sub_request(e2sm_buffer,gnb)

                    
            except KeyboardInterrupt:
                print("Data collection stopped.")

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