from imutils import build_montages
from datetime import datetime
import numpy as np
import imagezmq
import imutils
import cv2
import socket
import select
import time
import threading

class client:
    
    req_rep = False
    continue_requesting = False
    continue_procesing = False
    continue_sending = False
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = ('localhost', 9999)
    my_ip = 'localhost:5554'
    sender = None
    
    out = None
    path_out_num=0
    curr_frame=0
    
    fps = 1
    frame_buffer = []
    connect_to_port = None
    final_sent_frame = 0
    
    send_buffer = []
    
    def __init__(self):        
        i=0
        while True:
            # Send data
            print('sending message trial '+str(i)+'...')
            i+=1
            self.send_sock.sendto(("join||"+self.my_ip).encode('utf-8'), self.server_address)
            print('waiting to receive...')
            ready = select.select([self.send_sock], [], [], 10)
            if ready[0]:
                data, server = self.send_sock.recvfrom(4096)
                data = data.decode('utf-8')
                if "ok" in data:
                    self.connect_to_port = data.split("||")[1]
                    print("pub to : "+"tcp://*"+":"+self.my_ip.split(":")[1])
                    self.sender = imagezmq.ImageSender(connect_to="tcp://*"+":"+self.my_ip.split(":")[1],REQ_REP=False)
                    break
                
        self.continue_procesing = True
        processing = threading.Thread(target=self.worker)
        processing.start()
        
        #self.continue_sending = True
        #send_image_thread = threading.Thread(target=self.send_image)
        #send_image_thread.start()

    def requester(self,path):
        
        self.frame_buffer = []
        self.curr_frame = 0
        self.path_out = 'video'+str(self.path_out_num)+'.mp4'
        if self.out!=None:
            self.out.release()
        self.frame_buffer = []
        self.path_out_num+=1
        self.curr_frame=0
        rpiName = self.my_ip
        #vs = VideoStream(src=0).start()
        print(path)
        if path=="live":
            vs = cv2.VideoCapture(0)
        else:
            vs = cv2.VideoCapture(path)
        
        time.sleep(2.0)
        frame_number = 1
        
        self.send_buffer = []
        
        ret, frame = vs.read()
        width=400
        height=int((frame.shape[0]*width)/frame.shape[1])
        
        self.out = cv2.VideoWriter(self.path_out,cv2.VideoWriter_fourcc(*'XVID'), 30, (width,height))
        
        ret, frame = vs.read()
        
        while self.continue_requesting and ret:
            #print("sending",rpiName+"||request||"+str(frame_number))
            if self.sender!=None:
                self.sender.send_image(rpiName+"||request||"+str(frame_number), frame)
                #self.send_buffer.append((rpiName+"||request||"+str(frame_number), frame))
            else:
                print("sender is None")
            frame_number+=1
            ret, frame = vs.read()
            
        #print("done.")
        self.final_sent_frame=frame_number-1
        vs.release()
        
    def send_image(self):
        while self.continue_sending:
            if len(self.send_buffer)>0:
                msg,frame = self.send_buffer[0]
                self.sender.send_image(msg,frame)
                self.send_buffer.pop(0)
                #time.sleep(0.2)
        
    def worker(self):
        args = {"prototxt":"MobileNetSSD_deploy.prototxt.txt","model":"MobileNetSSD_deploy.caffemodel","montageW":2,"montageH":2,"confidence":0.2}
        print('sub to : '+self.server_address[0]+':'+self.connect_to_port)
        imageHub = imagezmq.ImageHub(open_port='tcp://'+self.server_address[0]+':'+self.connect_to_port,REQ_REP=False)
        
        CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat",
        	"bottle", "bus", "car", "cat", "chair", "cow", "diningtable",
        	"dog", "horse", "motorbike", "person", "pottedplant", "sheep",
        	"sofa", "train", "tvmonitor"]
        
        print("[INFO] loading model...")
        net = cv2.dnn.readNetFromCaffe(args["prototxt"], args["model"])
        
        
        CONSIDER = set(["person"])
        objCount = {obj: 0 for obj in CONSIDER}
        frameDict = {}
        
        lastActive = {}
        lastActiveCheck = datetime.now()
        
        ESTIMATED_NUM_PIS = 4
        ACTIVE_CHECK_PERIOD = 10
        ACTIVE_CHECK_SECONDS = ESTIMATED_NUM_PIS * ACTIVE_CHECK_PERIOD
        
        print("[INFO] detecting: {}...".format(", ".join(obj for obj in
        	CONSIDER)))
        #it=0
        while self.continue_procesing:
            (info, frame) = imageHub.recv_image()
            #imageHub.send_reply(b'OK')
            print("\n"+info)
            rpiName = info.split("||")[0]
            command = info.split("||")[1]
            frame_number = int(info.split("||")[2])
            if command=="processed":                
                if rpiName!=self.my_ip:
                    print("frame not mine.")
                else:
                    if frame_number == self.curr_frame+1:
                        self.out.write(frame)
                        self.curr_frame+=1
                        if self.final_sent_frame==frame_number:
                            if self.out!=None:
                                self.out.release()
                    elif frame_number>self.curr_frame:
                        self.frame_buffer.append((frame_number,frame))
                        
                    while True:
                        written = False
                        for (number,frame_i) in self.frame_buffer:
                            if number == self.curr_frame+1:
                                self.out.write(frame_i)
                                self.curr_frame+=1
                                self.frame_buffer.remove((number,frame_i))
                                if self.final_sent_frame==number:
                                    if self.out!=None:
                                        self.out.release()
                                written = True
                        if not written:
                            break
                    
            elif command=="request":
            	
                if rpiName not in lastActive.keys():
                    print("[INFO] receiving data from {}...".format(rpiName))
            	
                lastActive[rpiName] = datetime.now()
                
                frame = imutils.resize(frame, width=400)
                (h, w) = frame.shape[:2]
                blob = cv2.dnn.blobFromImage(cv2.resize(frame, (300, 300)),
            		0.007843, (300, 300), 127.5)
            	
                net.setInput(blob)
                detections = net.forward()
            	
                objCount = {obj: 0 for obj in CONSIDER}
                
                for i in np.arange(0, detections.shape[2]):
            		
                    confidence = detections[0, 0, i, 2]
            		
                    if confidence > args["confidence"]:
            			
                        idx = int(detections[0, 0, i, 1])
            			
                        if CLASSES[idx] in CONSIDER:
            				
                            objCount[CLASSES[idx]] += 1
            				
                            box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
                            (startX, startY, endX, endY) = box.astype("int")
            				
                            cv2.rectangle(frame, (startX, startY), (endX, endY),
            					(255, 0, 0), 2)
                cv2.putText(frame, rpiName, (10, 25),
            		cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
            
                label = ", ".join("{}: {}".format(obj, count) for (obj, count) in
            		objCount.items())
                cv2.putText(frame, label, (10, h - 20),
            		cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255,0), 2)
            	
                #frameDict[rpiName] = frame
            	
                if self.sender!=None:
                    self.sender.send_image(rpiName+"||processed||"+str(frame_number), frame)
                else:
                    print("sender is None")
                
                if (datetime.now() - lastActiveCheck).seconds > ACTIVE_CHECK_SECONDS:
            		
                    for (rpiName, ts) in list(lastActive.items()):
            			
                        if (datetime.now() - ts).seconds > ACTIVE_CHECK_SECONDS:
                            print("[INFO] lost connection to {}".format(rpiName))
                            lastActive.pop(rpiName)
                            frameDict.pop(rpiName)
            		
                    lastActiveCheck = datetime.now()

        cv2.destroyAllWindows()
        #imageHub.close_socket()
        del imageHub
    
    def become_requester(self,path):
        i=0
        while True:
            # Send data
            print('sending message trial '+str(i)+'...')
            i+=1
            self.send_sock.sendto(("request||"+self.my_ip).encode('utf-8'), self.server_address)
            print('waiting to receive...')
            ready = select.select([self.send_sock], [], [], 10)
            if ready[0]:
                data, server = self.send_sock.recvfrom(4096)
                data = data.decode('utf-8')
                if data=="ok":
                    break
        self.final_sent_frame = 0
        self.continue_requesting = True
        request = threading.Thread(target=self.requester,args=[path])
        request.start()
        
    def stop_requesting_thread(self):
        i=0
        while True:
            # Send data
            print('sending message trial '+str(i)+'...')
            i+=1
            self.send_sock.sendto(("stop||"+self.my_ip).encode('utf-8'), self.server_address)
            print('waiting to receive...')
            ready = select.select([self.send_sock], [], [], 10)
            if ready[0]:
                data, server = self.send_sock.recvfrom(4096)
                data = data.decode('utf-8')
                if data=="ok":
                    break
        self.continue_requesting = False
        #if self.out!=None:
        #    self.out.release()
        
    def exit_threads(self):
        i=0
        while True:
            # Send data
            print('sending message trial '+str(i)+'...')
            i+=1
            self.send_sock.sendto(("end||"+self.my_ip).encode('utf-8'), self.server_address)
            print('waiting to receive...')
            ready = select.select([self.send_sock], [], [], 10)
            if ready[0]:
                data, server = self.send_sock.recvfrom(4096)
                data = data.decode('utf-8')
                if data=="ok":
                    break
        self.continue_requesting = False
        self.continue_procesing = False
        self.continue_sending = False
        #self.sender.close_socket()
        if self.out!=None:
            self.out.release()
        
if __name__=='__main__':
    w = client()
    while True:
        a = input("\nEnter request to become requester or end to stop requesting or quit to exit\n")
        if "request" in a:
            path = a.split(" ")
            w.become_requester(path[1])
        elif a=="end":
            w.stop_requesting_thread()
        else:
            w.exit_threads()
            print("done.")
            break