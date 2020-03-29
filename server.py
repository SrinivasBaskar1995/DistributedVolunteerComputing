# import the necessary packages
from datetime import datetime
import imagezmq
import threading
import socket

class coordinator:
    
    continue_server = {}
    continue_listening = False
    
    my_ports = [x for x in range(5555,5600)]
    
    senders = {}
    clients = []
    port_to_client = {}
    
    req_rep = False
    
    def __init__(self):        
        self.continue_listening = True
        client_manager = threading.Thread(target=self.manager)
        client_manager.start()
        
    def server(self,port):
        imageHub = imagezmq.ImageHub(open_port='tcp://*:+'+str(port))
        
        frameDict = {}
        
        lastActive = {}
        lastActiveCheck = datetime.now()
        
        ESTIMATED_NUM_PIS = 4
        ACTIVE_CHECK_PERIOD = 10
        ACTIVE_CHECK_SECONDS = ESTIMATED_NUM_PIS * ACTIVE_CHECK_PERIOD
        
        iterations=0
        
        while port in self.continue_server.keys() and self.continue_server[port]:
            (info, frame) = imageHub.recv_image()
            rpiName = info.split("||")[0]
            command = info.split("||")[1]
            frame_number = int(info.split("||")[2])
            imageHub.send_reply(b'OK')
            
            print(info)
        	
            if rpiName not in lastActive.keys():
                print("[INFO] receiving data from {}...".format(rpiName))
        	
            lastActive[rpiName] = datetime.now()
        	
            frameDict[rpiName] = frame
         
            if (datetime.now() - lastActiveCheck).seconds > ACTIVE_CHECK_SECONDS:
        		
                for (rpiName, ts) in list(lastActive.items()):
        			
                    if (datetime.now() - ts).seconds > ACTIVE_CHECK_SECONDS:
                        print("[INFO] lost connection to {}".format(rpiName))
                        lastActive.pop(rpiName)
                        frameDict.pop(rpiName)
        		
                lastActiveCheck = datetime.now()
            
            if command=="request":
                print("sending to.",self.clients[iterations%len(self.clients)])
                self.senders[self.clients[iterations%len(self.clients)]].send_image(info, frame)
                iterations+=1
            elif command=="processed":
                print("sending to.",rpiName)
                self.senders[rpiName].send_image(info, frame)
            
        del imageHub
        
    def manager(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        my_ip = 'localhost'
        my_port = 9999
        
        server_address = (my_ip, my_port)
        print('\nlistening on %s port %s' % server_address)
        sock.bind(server_address)
        while self.continue_listening:
            message, client_address = sock.recvfrom(4096)
            message = message.decode('utf-8')
            print(message)
            if "join" in message:
                address = message.split("||")[1]
                sender = imagezmq.ImageSender(connect_to="tcp://"+address.split(":")[0]+":"+address.split(":")[1])
                self.senders[address] = sender
                self.clients.append(address)
                available_port = self.my_ports[0]
                self.continue_server[available_port] = True
                self.port_to_client[address] = available_port
                self.my_ports.pop(0)
                processing = threading.Thread(target=self.server,args=[available_port])
                processing.start()
                sock.sendto(("ok||"+str(available_port)).encode('utf-8'),client_address)
                
            elif "request" in message:
                address = message.split("||")[1]
                self.clients.remove(address)
                sock.sendto("ok".encode('utf-8'),client_address)
                
            elif "stop" in message:
                address = message.split("||")[1]
                self.clients.append(address)
                sock.sendto("ok".encode('utf-8'),client_address)
                
            elif "end" in message:
                address = message.split("||")[1]
                port = self.port_to_client[address]
                del self.port_to_client[address]
                self.continue_server.pop(available_port,None)
                self.my_ports.append(port)
                if address in self.clients:
                    self.clients.remove(address)
                if address in self.senders.keys():
                    del self.senders[address]
                sock.sendto("ok".encode('utf-8'),client_address)
        
        sock.close()
            
    def exit_threads(self):
        self.continue_server = {}
        self.continue_server = False
        self.continue_listening = False
            
if __name__=='__main__':
    c = coordinator()
    while True:
        a = input("\nEnter quit to exit\n")
        if a=="quit":
            c.exit_threads()
            print("done.")
            break