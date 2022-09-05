import sys
import time
import socket
import threading
from matplotlib import pyplot as plt

# print(sys.argv)

MSG_SIZE = 256

class Message:
    def __init__(self):
        self.SenderNodeName = ""
        self.Content = ""
        self.MessageID = ""
        self.priority = (0, "")
        self.deliverable = False
        self.needMulticast = False

    def set(self, msg_str):
        msg_list = msg_str.split('|')
        self.SenderNodeName = msg_list[0]
        self.Content = msg_list[1]
        self.MessageID = msg_list[2]    # id = node name + message timestamp when sending
        self.priority = eval(msg_list[3])     # the priority
        self.deliverable = False

    def get_message_string(self):
        # convert the class to string message so that you can send via network
        info_str = "%s|%s|%s|%s" %(self.SenderNodeName, self.Content, self.MessageID, self.priority)
        return info_str+"\0"*(MSG_SIZE-len(info_str))


def msg_sort_key(msg):
    return msg.priority

class ISISQueue:
    def __init__(self):
        self.queue = []
        self.feedback_table = dict()

    def sort(self):
        self.queue.sort(key=msg_sort_key, reverse=True)

    def print(self):
        print("======HEAD======")
        for msg in self.queue:
            print(msg.get_message_string().strip('\0')+'|'+str(msg.deliverable))
        print("======TAIL======")

    def append(self, msg):
        self.queue.append(msg)
        self.feedback_table[msg.MessageID] = [1, (0, "")] # [receiving time, max priority]
        self.sort()

    def delete(self, msg_id):
        for msg in self.queue:
            if msg.MessageID == msg_id:
                self.queue.remove(msg)
                self.feedback_table.pop(msg_id)
        self.sort()

    def deliver(self):
        delivered_msg = []
        while len(self.queue) != 0 and self.queue[-1].deliverable:
            self.feedback_table.pop(self.queue[-1].MessageID)
            delivered_msg.append(self.queue.pop(-1))
        return delivered_msg

    # update deliverability according to current node number
    def update_deliverability(self, node_num, node_name):
        for msg in self.queue:
            if self.feedback_table[msg.MessageID][0] == node_num + 1:
                msg.deliverable = True
                # if it is the feedback msg, update its priority as the agreed priority
                # then the node will multicast the msg with the agreed priority
                if msg.SenderNodeName == node_name:
                    l = self.feedback_table[msg.MessageID]
                    msg.priority = max(l[1], msg.priority)
                    msg.needMulticast = True
        return

    def update_priority(self, new_msg, node_num, node_name):
        agreed_priority = -1
        if new_msg.MessageID not in self.feedback_table:
            return agreed_priority
        l = self.feedback_table[new_msg.MessageID]
        l[0] += 1 # update receiving time
        # if it is feedback info
        if new_msg.SenderNodeName == node_name:
            l[1] = max(l[1], new_msg.priority)
        # if it is the agreed priority (receiving time == number of nodes)
        if l[0] == node_num + 1:
            for msg in self.queue:
                if msg.MessageID == new_msg.MessageID:
                    msg.priority = max(l[1], msg.priority)
                    agreed_priority = msg.priority
                    msg.deliverable = True
                    self.sort()
                    break
        return agreed_priority


isis_q = ISISQueue()
seen_msg = set()
send_socket = dict()
receive_socket = set()
balance_record = dict()
prop_priority = 0
bw_counter = 0
bw_logger = []
time_logger = []
node_terminate = False
delay_logger = dict()

isis_q_lock = threading.Lock()
seen_msg_lock = threading.Lock()
send_socket_lock = threading.Lock()
receive_socket_lock = threading.Lock()
balance_record_lock = threading.Lock()
prop_priority_lock = threading.Lock()
bw_counter_lock = threading.Lock()
delay_logger_lock = threading.Lock()

def receive_message(s, node_name):
    global isis_q
    global seen_msg
    global receive_socket
    global prop_priority
    global bw_counter
    # write code to wait until all the nodes are connected
    while not node_terminate:
        # use socket recv and than decode the message (eg. utf-8)
        data = s.recv(MSG_SIZE).decode('utf-8')
        if not data:
            break
        while len(data) < MSG_SIZE:
            data += s.recv(MSG_SIZE-len(data)).decode('utf-8')

        # record bandwidth
        bw_counter_lock.acquire()
        bw_counter += len(data)
        bw_counter_lock.release()
        
        msg = Message()
        msg.set(data.strip('\0'))
        # based on whether I have seen this message
        seen_msg_lock.acquire()
        if msg.MessageID in seen_msg:
            seen_msg_lock.release()
            # if I have seen the message, I could either be
            #   sender who get the feedback from other nodes or
            #   receiver who get msg from other nodes' R-multicast
            # But no matter whether I am sender or receiver, I will meet a msg n times, (n is the number of the node)
            # and I will get the agreed priority of this msg when I meet the msg at nth time
            isis_q_lock.acquire()
            receive_socket_lock.acquire()
            agreed_priority = isis_q.update_priority(msg, len(receive_socket), node_name)
            receive_socket_lock.release()
            if agreed_priority != -1:
                deliver()
                isis_q_lock.release()
                prop_priority_lock.acquire()
                prop_priority = max(prop_priority, agreed_priority[0])
                prop_priority_lock.release()
                if node_name == msg.SenderNodeName:
                    # sender have decided the agreed priority right now, multicast the agreed priority
                    msg.priority = agreed_priority
                    delay_logger_lock.acquire()
                    delay_logger[msg.MessageID].append(time.time())
                    delay_logger_lock.release()
                    multicast(msg, node_name)
                continue
            isis_q_lock.release()
        else:
            # if I have never seen this message, then I am not the sender,
            # I will deliver it and then multicast it
            # then every process knows my proposed priority, then can decide their own agreed priority for this message
            prop_priority_lock.acquire()
            prop_priority += 1
            msg.priority = (prop_priority, node_name)
            prop_priority_lock.release()
            seen_msg.add(msg.MessageID)
            isis_q_lock.acquire()
            isis_q.append(msg)
            isis_q_lock.release()
            seen_msg_lock.release()
            multicast(msg, node_name)

    receive_socket_lock.acquire()
    receive_socket.remove(s)
    node_num = len(receive_socket)
    s.close()
    fail_handler(node_num, node_name)
    receive_socket_lock.release()
    return

def get_events(node_name):
    global isis_q
    global seen_msg
    global prop_priority
    # write code to wait until all the nodes are connected 
    for line in sys.stdin:
        # init the message struct
        msg = Message()
        msg.SenderNodeName = node_name
        msg.Content = line.strip()
        msg.MessageID = node_name + str(time.time())
        prop_priority_lock.acquire()
        prop_priority += 1
        msg.priority = (prop_priority, node_name)
        prop_priority_lock.release()
        # register this message to some data structure to show that I have seen this message
        seen_msg_lock.acquire()
        seen_msg.add(msg.MessageID)
        seen_msg_lock.release()
        isis_q_lock.acquire()
        isis_q.append(msg)
        isis_q_lock.release()
        delay_logger_lock.acquire()
        delay_logger[msg.MessageID] = [time.time()]
        delay_logger_lock.release()
        multicast(msg, node_name)

def update_balances(msg_text):
    global balance_record

    parsed_msg = msg_text.split()
    operation = parsed_msg[0]

    if operation == "DEPOSIT":
        account = parsed_msg[1]
        fund = int(parsed_msg[2])
        balance_record_lock.acquire()
        if account in balance_record:
            balance_record[account] += fund
        else:
            balance_record[account] = fund
        balance_record_lock.release()
    elif operation == "TRANSFER":
        source = parsed_msg[1]
        destination = parsed_msg[3]
        fund = int(parsed_msg[4])
        if source not in balance_record:
            print("Source account does not exist!")
            return
        balance_record_lock.acquire()
        if balance_record[source] < fund:
            print("Invalid transaction! Source account does not have enough balance.")
            balance_record_lock.release()
            return
        balance_record[source] -= fund
        if destination in balance_record:
            balance_record[destination] += fund
        else:
            balance_record[destination] = fund
        balance_record_lock.release()
    else:
        print("Invalid message!")
        return
    
    balance_msg = "BALANCE"
    balance_record_lock.acquire()
    sorted_accounts = sorted(balance_record.keys())
    for account in sorted_accounts:
        balance_msg += " %s:%d"%(account, balance_record[account])
    balance_record_lock.release()
    print(balance_msg)

def deliver():
    global isis_q
    delivered_msg = isis_q.deliver()
    multicast_msg = []
    if len(delivered_msg) == 0:
        return
    for msg in delivered_msg:
        update_balances(msg.Content)
        if msg.needMulticast:
            multicast_msg.append(msg)
    # multicast agreed priority for feedback msgs that changed their deliverability when some nodes failed
    for msg in multicast_msg:
        delay_logger_lock.acquire()
        delay_logger[msg.MessageID].append(time.time())
        delay_logger_lock.release()
        multicast_without_check(msg)

def multicast(msg, node_name):
    global isis_q
    global send_socket
    for n in send_socket.copy():
        # send message, check if it has error
        need_close = False
        if n not in send_socket:
            continue
        s = send_socket[n]
        try:
            numbyte = s.send(msg.get_message_string().encode("utf-8"))
            if numbyte == 0:
                need_close = True
        except:
            need_close = True

        if need_close:
            # delete this connection
            send_socket_lock.acquire()
            send_socket.pop(n)
            node_num = len(send_socket)
            # close this socket
            s.close()
            fail_handler(node_num, node_name)
            send_socket_lock.release()

def fail_handler(node_num, node_name):
    # run deliver_queue_head() because a node is dead, maybe the queue's head don't have to wait for feedback
    isis_q_lock.acquire()
    isis_q.update_deliverability(node_num, node_name)
    deliver()
    isis_q_lock.release()

def multicast_without_check(msg):
    global send_socket
    for n in send_socket.copy():
        # send message, check if it has error
        if n not in send_socket:
            continue
        s = send_socket[n]
        try:
            s.send(msg.get_message_string().encode("utf-8"))
        except:
            continue

def node_connect(id, addr, port):
    global send_socket
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((addr, port))
            send_socket_lock.acquire()
            send_socket[id] = s
            send_socket_lock.release()
            break
        except:
            continue

def read_config(filename):
    f = open(filename, "r")
    node_num = int(f.readline().strip())
    node_info = []
    for line in f:
        info = line.strip().split()
        info[2] = int(info[2])
        node_info.append(info)
    return node_num, node_info

def bandwidth_logger(interval):
    global bw_counter
    global bw_logger
    
    start_time = time.time()
    time_logger.append(0)
    while not node_terminate:
        time.sleep(interval)
        curr_time = time.time() - start_time
        time_diff = curr_time - time_logger[-1]
        time_logger.append(curr_time)
        bw_counter_lock.acquire()
        bw_logger.append(bw_counter/time_diff)
        bw_counter = 0
        bw_counter_lock.release()

def main():
    global receive_socket
    global node_terminate

    if len(sys.argv) != 4:
        print('Incorrect input arguments')
        sys.exit(0)
    node_name = sys.argv[1]
    port = int(sys.argv[2])
    host = '127.0.0.1' # can connect with any ip
    config_fname = sys.argv[3]

    # read condig information from file
    node_num, node_info = read_config(config_fname)

    # connect other nodes
    for info in node_info:
        connect_t = threading.Thread(target=node_connect, args=(info[0], info[1], info[2]))
        connect_t.start()

    # listen other nodes
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(64)
    while len(receive_socket) != node_num:
        sock, addr = s.accept()
        receive_socket.add(sock)

    # check whether all nodes are connected
    while True:
        # send_socket_lock.acquire()
        if len(send_socket) == node_num:
            # send_socket_lock.release()
            break
        # send_socket_lock.release()

    # start receiving message
    for ss in receive_socket:
        receive_t = threading.Thread(target=receive_message, args=(ss, node_name))
        receive_t.start()

    interval = 1
    show_figure = False
    bw_log = threading.Thread(target=bandwidth_logger, args=(interval,))
    bw_log.start()

    # start sending message
    # send_t = threading.Thread(target=get_events(node_name))
    # send_t.start()
    while True:
        try:
            get_events(node_name)
        except:
            node_terminate = True
            sorted_msg_id = sorted(delay_logger, key = lambda x:delay_logger[x][0])
            msg_delay = []
            for msg in sorted_msg_id:
                if len(delay_logger[msg]) == 2:
                    msg_delay.append(delay_logger[msg][1]-delay_logger[msg][0])

            plt.figure(figsize=(6,10))
            # plot bandwidth
            bw_fig = plt.subplot(211)
            bw_fig.plot(time_logger[1:], bw_logger)
            bw_fig.set_title("Bandwidth for "+node_name)
            bw_fig.set_xlabel("Time / s")
            bw_fig.set_ylabel("Bandwidth / Byte/s")
            # plot message processing delay
            delay_fig = plt.subplot(212)
            delay_fig.plot(list(range(len(msg_delay))),msg_delay)
            delay_fig.set_title("Message processing delay for "+node_name)
            delay_fig.set_xlabel("Message ID")
            delay_fig.set_ylabel("Delay / s")
            plt.savefig("./result/%s.png"%(node_name))
            if show_figure:
                plt.show()
            break

if __name__ == "__main__":
    main()