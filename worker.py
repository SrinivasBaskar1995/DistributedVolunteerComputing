import numpy as np
import imagezmq
import imutils
import cv2
import socket
import select
import time
import threading
from PIL import Image
import sys

class client:
    
    verbose = False
    req_rep = True
    number_of_frames_in_chunk = 100
    max_buffer = 4000
    continue_requesting = False
    continue_procesing = False
    continue_sending = False
    continue_receiving = False
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = ('localhost', 9999)
    my_ip = ''
    my_port = '5554'
    sender = None
    
    out = None
    path_out_num=0
    curr_frame=0
    
    fps = 1
    frame_buffer = []
    connect_to_port = None
    final_sent_frame = 0
    
    send_buffer = []
    recv_buffer = []
    
    start_time = 0
    
    def log(self,message):
        if self.verbose:
            print(message)
    
    def __init__(self,server_ip='localhost',own_ip='localhost'):
        self.server_address = (server_ip,9999)
        self.my_ip = own_ip+":"+self.my_port
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
                    if self.req_rep:
                        print("sending to : "+"tcp://*"+":"+self.my_ip.split(":")[1])
                        self.sender = imagezmq.ImageSender(connect_to="tcp://"+self.server_address[0]+":"+self.connect_to_port)
                    else:
                        print("sending to : "+"tcp://*"+":"+self.my_ip.split(":")[1])
                        self.sender = imagezmq.ImageSender(connect_to="tcp://*"+":"+self.my_ip.split(":")[1],REQ_REP=False)
                    break
                
        self.continue_procesing = True
        processing = threading.Thread(target=self.worker)
        processing.start()
        
        self.continue_sending = True
        send_image_thread = threading.Thread(target=self.send_image_thread)
        send_image_thread.start()
        
        self.continue_receiving = True
        recv_image_thread = threading.Thread(target=self.recv_image_thread)
        recv_image_thread.start()

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
        self.final_sent_frame = 0
        
        if path=="live":
            vs = cv2.VideoCapture(0)
        else:
            vs = cv2.VideoCapture(path)
        
        time.sleep(2.0)
        frame_number = 1
        
        self.send_buffer = []
        
        self.start_time = time.time()
        
        ret, frame = vs.read()
        width=400
        height=int((frame.shape[0]*width)/frame.shape[1])
        self.out = cv2.VideoWriter(self.path_out,cv2.VideoWriter_fourcc(*'mp4v'), 30, (width,height))
        
        ret, frame = vs.read()
        
        while self.continue_requesting and ret:
            self.send_buffer.append((rpiName+"||request||"+str(frame_number), frame))
            frame_number+=1
            ret, frame = vs.read()
            while len(self.send_buffer)>self.max_buffer:
                time.sleep(0.1)
            if not self.req_rep:
                time.sleep(0.05)
            
        if not ret:
            self.stop_requesting_thread()
        self.final_sent_frame=frame_number-1
        self.final_sent_frame-=self.final_sent_frame%self.number_of_frames_in_chunk
        print("final frame sent : "+str(self.final_sent_frame)+"\n")
        vs.release()
        print("requester terminated.")
        
    def send_image_thread(self):
        while self.continue_sending:
            if len(self.send_buffer)>=self.number_of_frames_in_chunk:
                msg,frame = self.send_buffer[0]
                parts = msg.split("||")
                new_msg = parts[0]+"||"+parts[1]+"||"
                height = frame.shape[0]
                width = frame.shape[1]
                command = parts[1]
                rpiName = parts[0]
                dst = Image.new('RGB', (self.number_of_frames_in_chunk*width, height))
                index=0
                for i in range(self.number_of_frames_in_chunk):
                    if index>=len(self.send_buffer):
                        break
                    msg,frame = self.send_buffer[index]
                    if msg.split("||")[0]==rpiName and msg.split("||")[1]==command and frame.shape[0]==height and frame.shape[1]==width:
                        new_msg+=msg.split("||")[2]+"-"
                        im_pil = Image.fromarray(frame)
                        dst.paste(im_pil, (i*width, 0))
                        self.send_buffer.pop(index)
                    else:
                        index+=1
                new_msg = new_msg[:len(new_msg)-1]
                new_msg+="||"+str(height)+"||"+str(width)
                chunk = np.asarray(dst)
                self.sender.send_image(new_msg,chunk)
        #self.sender.close_socket()
        print("send_image_thread terminated.")
                
    def recv_image_thread(self):
        if self.req_rep:
            print('receiving at : '+'tcp://*'+':'+self.my_ip.split(":")[1])
            imageHub = imagezmq.ImageHub(open_port='tcp://*'+':'+self.my_ip.split(":")[1])
        else:
            print('receiving at : '+'tcp://'+self.server_address[0]+':'+self.connect_to_port)
            imageHub = imagezmq.ImageHub(open_port='tcp://'+self.server_address[0]+':'+self.connect_to_port,REQ_REP=False)
        while self.continue_receiving:
            while self.req_rep and len(self.recv_buffer)>self.max_buffer:
                time.sleep(0.1)
            (info, frame) = imageHub.recv_image()
            data = info.split("||")
            frames = data[2].split("-")
            for i in range(len(frames)):
                crop_img = frame[0:int(data[3]), i*int(data[4]):(i+1)*int(data[4])]
                new_info = data[0]+"||"+data[1]+"||"+frames[i]
                self.recv_buffer.append((new_info, crop_img))
                self.log(new_info)
            if self.req_rep:
                imageHub.send_reply(b'OK')
        #imageHub.close_socket()
        del imageHub
        print("recv_image_thread terminated.")
        
    def worker(self):
        args = {"prototxt":"MobileNetSSD_deploy.prototxt.txt","model":"MobileNetSSD_deploy.caffemodel","montageW":2,"montageH":2,"confidence":0.2}
        
        CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat",
        	"bottle", "bus", "car", "cat", "chair", "cow", "diningtable",
        	"dog", "horse", "motorbike", "person", "pottedplant", "sheep",
        	"sofa", "train", "tvmonitor"]
        
        print("[INFO] loading model...")
        net = cv2.dnn.readNetFromCaffe(args["prototxt"], args["model"])
        
        
        CONSIDER = set(["person"])
        objCount = {obj: 0 for obj in CONSIDER}
        
        print("[INFO] detecting: {}...".format(", ".join(obj for obj in
        	CONSIDER)))
        while self.continue_procesing:
            if len(self.recv_buffer)==0:
                continue
            (info,frame) = self.recv_buffer[0]
            self.recv_buffer.pop(0)
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
                        self.log("processed frame : "+str(frame_number))
                        if self.final_sent_frame==frame_number:
                            print("final frame time taken for the job = "+str(time.time()-self.start_time))
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
                                self.log("processed frame : "+str(number))
                                self.frame_buffer.remove((number,frame_i))
                                if self.final_sent_frame==number:
                                    print("final frame time taken for the job = "+str(time.time()-self.start_time))
                                    if self.out!=None:
                                        self.out.release()
                                written = True
                        if not written:
                            break
                    
            elif command=="request":
                
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
            	
                while self.req_rep and len(self.send_buffer)>self.max_buffer:
                    time.sleep(0.1)
                self.send_buffer.append((rpiName+"||processed||"+str(frame_number), frame))

        cv2.destroyAllWindows()
        print("worker terminated.")
    
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
        self.continue_receiving = False
        if self.out!=None:
            self.out.release()
        
if __name__=='__main__':
    if len(sys.argv)>1:
        w = client(sys.argv[1],sys.argv[2])
    else:
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