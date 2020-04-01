import imagezmq
import threading
import socket
import time
import sys

class coordinator:
    
    verbose = False
    req_rep = True
    my_ip = 'localhost:9999'
    max_buffer = 40
    continue_server = {}
    continue_listening = False
    continue_send_request = False
    continue_send_reply = {}
    
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
        
    def server(self,ip,port):
        if self.req_rep:
            print('receiving at : '+'tcp://*'+':'+str(port))
            imageHub = imagezmq.ImageHub(open_port='tcp://*'+':'+str(port))
        else:
            print('receiving at : '+'tcp://'+ip+':'+str(port))
            imageHub = imagezmq.ImageHub(open_port='tcp://'+ip+':'+str(port),REQ_REP=False)
        while port in self.continue_server.keys() and self.continue_server[port]:
            (info, frame) = imageHub.recv_image()
            self.log(info)
            rpiName = info.split("||")[0]
            command = info.split("||")[1]
            
            if command=="request":
                while self.req_rep and len(self.requests)>self.max_buffer:
                    time.sleep(0.1)
                self.requests.append((info, frame))
            elif command=="processed":
                while self.req_rep and len(self.client_result)>self.max_buffer:
                    time.sleep(0.1)
                self.client_result[rpiName].append((info, frame))
            if self.req_rep:
                imageHub.send_reply(b'OK')
        
        #imageHub.close_socket()
        del imageHub
        print("server terminated.")
        
    def send_reply(self,rpiName):
        while rpiName in self.continue_send_reply.keys() and self.continue_send_reply[rpiName]:
            if len(self.client_result[rpiName])>0:
                info, frame = self.client_result[rpiName][0]
                self.client_result[rpiName].pop(0)
                self.senders[rpiName].send_image(info, frame)
                #time.sleep(0.05)
        print("send_reply terminated.")
                
    def send_request(self):
        iterations=0
        while self.continue_send_request:
            if len(self.requests)>0:
                info,frame = self.requests[0]
                self.requests.pop(0)
                rpiName = info.split("||")[0]
                clients = []
                for client in self.clients:
                    if client!=rpiName:
                        clients.append(client)
                if len(clients)>0:
                    self.senders[clients[iterations%len(clients)]].send_image(info, frame)
                    iterations+=1
        print("send_request terminated.")
        
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
                    sock.sendto("ok".encode('utf-8'),client_address)
            except socket.timeout:
                continue
        print("manager terminated.")
        sock.close()
            
    def exit_threads(self):
        self.continue_server = {}
        self.continue_listening = False
        self.continue_send_request = False
            
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