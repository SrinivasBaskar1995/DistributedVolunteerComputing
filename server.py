import imagezmq
import threading
import socket
import time
import sys
import signal

class Patience:
    class Timeout(Exception):
        pass

    def __init__(self, seconds):
        self.seconds = seconds

    def __enter__(self):
        if hasattr(signal, "SIGALRM"):
            signal.signal(signal.SIGALRM, self.raise_timeout)
            signal.alarm(self.seconds)

    def __exit__(self, *args):
        if hasattr(signal, "alarm"):
            signal.alarm(0)

    def raise_timeout(self, *args):
        raise Patience.Timeout()

class coordinator:
    
    verbose = False
    req_rep = True
    my_ip = 'localhost:9999'
    max_buffer = 4000
    number_of_frames_in_chunk = 100
    continue_server = {}
    continue_listening = False
    continue_send_request = False
    continue_check_timeout = False
    continue_send_reply = {}
    client_sent = {}
    client_time = {}
    client_resources = {}
    timeout = 6
    minimum_battery = 20
    
    my_ports = [x for x in range(5555,5600)]
    
    senders = {}
    clients = []
    client_result = {}
    port_to_client = {}
    requests = []
    
    def log(self,message):
        if self.verbose:
            print(message)
    
    def __init__(self,ip='localhost'):
        self.my_ip = ip+":9999"
        self.continue_listening = True
        client_manager = threading.Thread(target=self.manager)
        client_manager.start()
        
        self.continue_send_request = True
        request_thread = threading.Thread(target=self.send_request)
        request_thread.start()
        
        self.continue_check_timeout = True
        chk_timeout = threading.Thread(target=self.check_timeout)
        chk_timeout.start()
        
    def server(self,ip,port):
        if self.req_rep:
            print('receiving at : '+'tcp://*'+':'+str(port))
            imageHub = imagezmq.ImageHub(open_port='tcp://*'+':'+str(port))
        else:
            print('receiving at : '+'tcp://'+ip+':'+str(port))
            imageHub = imagezmq.ImageHub(open_port='tcp://'+ip+':'+str(port),REQ_REP=False)
        while port in self.continue_server.keys() and self.continue_server[port]:
            try:
                with Patience(self.timeout):
                    (info, frame) = imageHub.recv_image()
            except Patience.Timeout:
                print("timeout")
                continue
            self.log(info)
            rpiName = info.split("||")[0]
            command = info.split("||")[1]
            
            if command=="request":
                while self.req_rep and len(self.requests)>self.max_buffer/self.number_of_frames_in_chunk:
                    time.sleep(0.1)
                self.requests.append((info, frame))
            elif command=="processed":
                received_from = rpiName.split("-")[1]
                while self.req_rep and len(self.client_result)>self.max_buffer/self.number_of_frames_in_chunk:
                    time.sleep(0.1)
                self.client_result[rpiName.split("-")[0]].append((info, frame))
                if (info, frame) in self.client_sent[received_from]:
                    self.client_sent[received_from].remove((info, frame))
            if self.req_rep:
                imageHub.send_reply(b'OK')
        
        #imageHub.close_socket()
        del imageHub
        print("server terminated.")
        
    def send_reply(self,rpiName):
        while rpiName in self.continue_send_reply.keys() and self.continue_send_reply[rpiName]:
            try:
                with Patience(self.timeout):
                    if len(self.client_result[rpiName])>0:
                        info, frame = self.client_result[rpiName][0]
                        self.senders[rpiName].send_image(info, frame)
                        self.client_result[rpiName].pop(0)
            except Patience.Timeout:
                print("timeout")
                #time.sleep(0.05)
        print("send_reply terminated.")
                
    def send_request(self):
        iterations=0
        while self.continue_send_request:
            try:
                with Patience(self.timeout):
                    if len(self.requests)>0:
                        info,frame = self.requests[0]
                        rpiName = info.split("||")[0]
                        clients = []
                        for client in self.clients:
                            if client!=rpiName:
                                clients.append(client)
                        if len(clients)>0:
                            if clients[iterations%len(clients)] not in self.client_sent.keys():
                                self.client_sent[clients[iterations%len(clients)]] = []
                            if len(self.client_sent[clients[iterations%len(clients)]])<self.client_resources[clients[iterations%len(clients)]]:
                                self.senders[clients[iterations%len(clients)]].send_image(info, frame)
                                self.client_sent[clients[iterations%len(clients)]].append((info, frame))
                                self.requests.pop(0)
                                iterations+=1
            except Patience.Timeout:
                print("timeout")
        print("send_request terminated.")
        
    def check_timeout(self):
        while self.continue_check_timeout:
            failed = []
            for client in self.client_time.keys():
                if time.time()-self.client_time[client]>self.timeout:
                    failed.append(client)
            for client in failed:
                self.log("failed : "+client)
                del self.client_time[client]
                self.client_failed(client)
        
    def client_failed(self,client_add):
        if client_add in self.client_sent.keys():
            for info,frame in self.client_sent[client_add]:
                while self.req_rep and len(self.requests)>self.max_buffer/self.number_of_frames_in_chunk:
                    time.sleep(0.1)
                self.requests.append((info, frame))
        
        if client_add in self.client_sent.keys():
            del self.client_sent[client_add]
            
        if client_add in self.continue_send_reply.keys():
            del self.continue_send_reply[client_add]
            
        remove_messages = []
        for info,frame in self.requests:
            if info.split("||")[0]==client_add:
                remove_messages.append((info,frame))
        
        for info,frame in remove_messages:
            self.requests.remove((info,frame))
        
        if client_add in self.port_to_client.keys():
            port = self.port_to_client[client_add]
            del self.port_to_client[client_add]
            if port in self.continue_server.keys():
                self.continue_server.pop(port,None)
            if port not in self.my_ports:
                self.my_ports.append(port)
        if client_add in self.continue_send_reply.keys():
            self.continue_send_reply.pop(client_add,None)
        if client_add in self.clients:
            self.clients.remove(client_add)
        if client_add in self.senders.keys():
            #self.senders[address].close_socket()
            del self.senders[client_add]
        
    def manager(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        server_address = ('', int(self.my_ip.split(":")[1]))
        print('\nlistening on %s port %s' % server_address)
        sock.bind(server_address)
        sock.settimeout(2)
        while self.continue_listening:
            try:
                message, client_address = sock.recvfrom(4096)
                message = message.decode('utf-8')
                if "join" in message:
                    address = message.split("||")[1]
                    self.clients.append(address)
                    available_port = self.my_ports[0]
                    self.port_to_client[address] = available_port
                    self.my_ports.pop(0)
                    if self.req_rep:
                        print("sending to : "+"tcp://"+address.split(":")[0]+":"+address.split(":")[1])
                        sender = imagezmq.ImageSender(connect_to="tcp://"+address.split(":")[0]+":"+address.split(":")[1])
                        self.senders[address] = sender
                        self.continue_server[available_port] = True
                        processing = threading.Thread(target=self.server,args=['',available_port])
                        processing.start()
                    else:
                        print("sending to : "+"tcp://*"+":"+str(available_port))
                        sender = imagezmq.ImageSender(connect_to="tcp://*"+":"+str(available_port),REQ_REP=False)
                        self.senders[address] = sender
                        self.continue_server[address.split(":")[1]] = True
                        processing = threading.Thread(target=self.server,args=[address.split(":")[0],address.split(":")[1]])
                        processing.start()
                    sock.sendto(("ok||"+str(available_port)).encode('utf-8'),client_address)
                    
                elif "request" in message:
                    address = message.split("||")[1]
                    self.clients.remove(address)
                    self.client_result[address] = []
                    if address not in self.continue_send_reply.keys() or not self.continue_send_reply[address]:
                        self.continue_send_reply[address]= True
                        processing = threading.Thread(target=self.send_reply,args=[address])
                        processing.start()
                    sock.sendto("ok".encode('utf-8'),client_address)
                    
                elif "stop" in message:
                    address = message.split("||")[1]
                    self.clients.append(address)
                    sock.sendto("ok".encode('utf-8'),client_address)
                    
                elif "end" in message:
                    address = message.split("||")[1]
                    port = self.port_to_client[address]
                    del self.port_to_client[address]
                    self.continue_server.pop(port,None)
                    self.my_ports.append(port)
                    if address in self.continue_send_reply.keys():
                        self.continue_send_reply.pop(address,None)
                    if address in self.clients:
                        self.clients.remove(address)
                    if address in self.senders.keys():
                        #self.senders[address].close_socket()
                        del self.senders[address]
                    if address in self.client_resources:
                        del self.client_resources[address]
                    sock.sendto("ok".encode('utf-8'),client_address)

                elif "ping" in message:
                    address = message.split("||")[1]
                    cpu_left = float(message.split("||")[2])
                    ram_left = float(message.split("||")[3])
                    battery_percent = float(message.split("||")[4])
                    self.log("ping from : "+address)
                    self.client_time[address]=time.time()
                    if battery_percent<self.minimum_battery or cpu_left<0 or ram_left<100000000:
                        self.client_resources[address]=0
                    else:                        
                        self.client_resources[address]=10000

            except socket.timeout:
                continue
        print("manager terminated.")
        sock.close()
            
    def exit_threads(self):
        self.continue_server = {}
        self.continue_listening = False
        self.continue_send_request = False
        self.continue_check_timeout = False
            
if __name__=='__main__':
    if len(sys.argv)>1:
        c = coordinator(sys.argv[1])
    else:
        c = coordinator()
    while True:
        a = input("\nEnter quit to exit\n")
        if a=="quit":
            c.exit_threads()
            print("done.")
            break