#zID: z5060611
#Submission Date: 26.04.2019
#Description: The following code is used to create a simple DHT with the ability to transmit files.
#Python Version: python3
#Youtube Link: https://youtu.be/oZVKDajXH4M

import socket
import sys
import time
import threading
import pickle
import multiprocessing
import random

from multiprocessing import Process, Manager


#Create the log files for responding and requesting peers
def logs(event, msg, filename):
        
        timer = round((time.time()-start_time),2) #Time since start
        f = open(filename, "a+")    
        f.write("{:10s} {:10s} {:10s} {:10s} {:10s} \n" .format(str(event), str(timer), str(msg['Sequence Number']), str(msg['Bytes']), str(msg['Ack'])))   
        f.close()
#Print and update if there has been a change to first and second successors
def UpdateSuccessor(SuccessorChange):

    global TCP_Client, Successor_1, Successor_2, port_FS, port_SS

    if(SuccessorChange['First']):
        print("My first successor is now peer {0}".format(port_FS-address_offset ))
        Successor_1 = int(port_FS-address_offset)
        TCP_Client.disconnect()
        time.sleep(0.1)
        TCP_Client = TCPClient(host, port_FS, 5)
        TCP_Client.establish_connection()
    if(SuccessorChange['Second']):
        if(SuccessorChange['First'] == False):
            print("My first successor is now peer {0}".format(port_FS-address_offset ))
        print("My second successor is now peer {0}".format(port_SS-address_offset ))
        Successor_2 = int(port_SS - address_offset)

#Check which peer or the next successor has the file
#****Code****
# 0 = File Request
# 1 = The next successor has the file
# 2 = Request Message
# 3 = Graceful Departure
# 4 = Predecessor asking for successors
def CodeSorter(Message):

    global port_FS, port_SS

    flag = False   #True if peer has the file
    hash_no = int(Message['File'])%256
    SuccessorChange = {'First':False, 'Second':False}
    
    #7.2.2: Forwarding the request to the correct node that has requested the file
    if(Message['Code'] == 1): #Current peer has file
        flag = True          
    elif(Message['Code'] == 0): #Check if current peer has file
        if(int(peerID) == hash_no):
            flag = True
        elif (int(peerID) < hash_no <= int(Successor_1)): #Successor has file
            Message['Code'] = 1
            TCP_Client.sendData(Message)
        elif(hash_no > int(peerID) >= int(Successor_1)):
            if((hash_no - int(peerID)) < (256+int(Successor_1) - hash_no)):
                flag = True
            else:
                Message['Code'] = 1 #Successor has file
                TCP_Client.sendData(Message)
        else:
            TCP_Client.sendData(Message)    #Both peer and successor don't have file...pass it on
    elif(Message['Code'] == 2): #Request message received, start reading the file!
        print("Received a response message from peer {0}, which has the file {1}." .format(Message['Responding Port'] - address_offset, Message['File']))
        print("We now start receiving the file ......")
        UDP_FT.reliableRead() #Reliable UDP for reading file
        print("The file is received")
    elif(Message['Code']==3):  #Graceful departure
        if(Message['Port'] == port_FS):
            Message['Type'] = "Ping"

            UDPsock.sendData(Message, host, port_FS) #Send response to departing peer
            print("Peer {0} will depart from the network.".format(Message['Port']-address_offset))

            Message['Code'] = 4
            Message['Successor'] = 1

            #7.3.3: Choosing correct successors after departing node
            Message['PortFS'] = Message['PortSS']
            UDPsock.sendData(Message, host, port) #Send to myself to transfer ports out of process

        elif(Message['Port'] == port_SS):
            Message['Type'] = "Ping"

            UDPsock.sendData(Message, host, port_SS) #Send response to departing peer
            print("Peer {0} will depart from the network.".format(Message['Port']-address_offset ))
            Message['Code'] = 4
            Message['Successor'] = 2

            #7.3.3: Choosing correct successors after departing node
            Message['PortSS'] = Message['PortFS']
            UDPsock.sendData(Message, host, port) #Send to myself to transfer ports out of process


    elif (Message['Code'] == 4): #Asking for successors
        Message['Type'] = "Ping"
        if(Message['Successor'] == 1):
            Message['PortFS'] = port_FS
            Message['Successor'] = 1
        else:
            Message['PortSS'] = port_SS
            Message['Successor'] = 2
        UDPsock.sendData(Message, host, Message['Port'])

    
    UpdateSuccessor(SuccessorChange)
            
            
    if(flag): #Peer has the file
        print("File {0} is here." .format(Message['File']))
        Message['Code'] = 2
        Message['Responding Port'] = port

        #7.2.3: Sending the response directly to the requester using TCP
        TCP_FT_Response = TCPClient(host, Message['Requesting Port'], 5)
        time.sleep(0.5)
        TCP_FT_Response.establish_connection()
        TCP_FT_Response.sendData(Message)
        print("A response message, destined for peer {0}, has been sent" .format(Message['Requesting Port']-address_offset))

        time.sleep(10.0) # Wait an appropriate amount of time
        print("We now start sending the file ......")
        UDP_FT.reliableSend(str(Message['File'])+".pdf", host, Message['Requesting Port']+FT_offset, 2) #Send the file via UDP
        print("The file is sent")
        TCP_FT_Response.disconnect()
    elif(Message['Code'] == 0 or Message['Code'] == 1):
        print("File {0} is not stored here.".format(Message['File']))
        print("File request message has been forwarded to my successor.")
    


#Process runs in the background listenting for data for TCP connection 
def handle(connection, address):
    try:
        while True:
            data = pickle.loads(connection.recv(1024))
            if not data: continue
            CodeSorter(data)
    except Exception:
        pass
    finally:
        connection.close()

#Class creates UDP sockets, handling both sending and reading of data 
class UDP():

    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket = None
        self.socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)    

    #Connect to socket for listening    
    def connect(self):
        self.socket.bind((self.host, self.port)) 
        self.socket.settimeout(self.timeout)
    
    #Send data - unreliable UDP    
    def sendData(self, data, host, port):
        self.socket.sendto(pickle.dumps(data), (host, port))

    #Read data - unreliable UDP   
    def readData(self):
        try:
            data, addr = self.socket.recvfrom(1024)
            flag = True #Flag = true if receiving from socket has been successful
            return (flag, data, addr)
        except socket.timeout:
            flag = False
            return (flag, flag, flag)

    #Reliable UDP receiver for file transfer
    #Main purpose is to receive packets, return acks and log transmissions
    #Transmission Codes:
    # 0  = End of Transmissions     
    def reliableRead(self):
        global Reliable_UDP_msg
        msg = Reliable_UDP_msg
        f = open("received_file.pdf", "wb")
        ack = 1
        
        while True: #Listen for data packets
            try:
                data, addr = self.socket.recvfrom(1024)
            except socket.timeout:
                continue

            rcv_msg = pickle.loads(data)

            if (rcv_msg['Code'] == 0): #File transmission finished - stop listening
                    break
            if(rcv_msg['Sequence Number'] == ack):
                logs("rcv", rcv_msg, "requesting_log.txt" ) #Log data packet received
                f.write(rcv_msg['Payload']) #Append data to file
                ack = ack+rcv_msg['Bytes']
            
                msg['Event'] = "Snd"
                msg['Time'] = round((time.time() - start_time),2)
                msg['Sequence Number'] = 0
                msg['Bytes'] = rcv_msg['Bytes']
                msg['Ack'] = rcv_msg['Bytes']+rcv_msg['Sequence Number']
                msg['Sending Port'] = self.port
                msg['Payload'] = rcv_msg['Payload']
                

                logs(msg['Event'], msg, "requesting_log.txt") #Log ack being sent

                self.socket.sendto(pickle.dumps(msg), (host, int(rcv_msg['Sending Port']))) # Send ack


            f.truncate()
        f.close()
   
    #Reliable UDP sender for file transfer
    #Main purpose is to implement stop and wait protocol and log transmissions
    #Transmission Codes:
    # 0  = End of Transmissions
    def reliableSend(self, filename, host, port_req, timer):
    
        global Reliable_UDP_msg
        with open(filename, "rb") as f:
            buff = f.read(MSS) #Read MSS Bytes of data from file
            RTXFlag = False #Flag is false if not retransmitting packet
            SequenceNumber = 1 #Begin sequence number
            
            #Create nessage header
            msg = Reliable_UDP_msg
            msg['Event'] = "Snd"
            msg['Sequence Number'] = SequenceNumber
            msg['Bytes'] = len(buff)
            msg['Ack'] = 0
            msg['Sending Port'] = self.port

            while (buff):
                msg['Payload'] = buff #Store buff in data packet
                msg['Time'] = round((time.time() - start_time),2) #Update time
                
                #7.2.6: Implement packet loss
                if(random.uniform(0,1) > float(DropProb)):
                    if (RTXFlag):
                        msg['Event'] = "Rtx"
                        RTXFlag = False
                    self.socket.sendto(pickle.dumps(msg), (host, int(port_req)))
                    logs(msg['Event'], msg, "responding_log.txt")
                

                curr_time = time.time() #Update current time to check packet drop

                while True:
                    
                    #7.2.7: Segments are transmitted after timeout
                    if ((time.time()-curr_time) > 1): #If it has been 1 second since ack then assume packet loss
                        if (RTXFlag): #If the RTXflag is already on then the retransmission has also been lost
                            logs("RTX/Drop", msg, "responding_log.txt" )
                            break
                        logs("Drop", msg, "responding_log.txt" )
                        RTXFlag = True
                        break

                    #7.2.5: Stop-and-wait behaviour
                    try:
                        data, addr = self.socket.recvfrom(1024)
                    except socket.timeout:
                        continue
                    
                    rcv_msg = pickle.loads(data)

                    if (rcv_msg['Ack'] == (SequenceNumber+len(buff))): #Received ack for data transmission     
                            logs("rcv", rcv_msg, "responding_log.txt")
                            SequenceNumber = SequenceNumber+len(buff)
                            buff = f.read(MSS)

                            msg['Event'] = "Snd"
                            msg['Sequence Number'] = SequenceNumber
                            msg['Bytes'] = len(buff)
                            
                            break
                    else:
                            break

            msg['Code'] = 0 #Send end of transmission code
            self.socket.sendto(pickle.dumps(msg), (host, int(port_req)))
            
            
#7.1 Ping Successors (2 marks)
#Transmission Codes:
#0 = Ping Response
#3 = Asking for a successor
#4 = Response from #3
#6 = Response form graceful departure request
#8 = Ping Request
class ping(threading.Thread):
    
    def run(self):
        time_PING = 0
        #Sequence numbers for when a peer leaves
        FS_Resp_Seq = 1 
        SS_Resp_Seq = 1
        SequenceNumber = 1

        global port_FS, port_SS, thread_stop, Successor_1, Successor_2, TCP_Client, port_pre1, port_pre2, TCP_Depart_Pre1, TCP_Depart_Pre2, TCP_msg

        #Continously sending and receving requests/responses
        while True:

                msg = {'Type':"Ping",'Code':True,'Sequence Number':True, 'Successor':False, 'Port':True}
                SuccessorChange = {'First':False, 'Second':False}
                
                if (thread_stop == 2): #If the peer has decided to leave, then exit while loop
                    break

                if (time.time() - time_PING > ping_delay): #Send pings every "ping_delay" seconds
                    #7.4.1: Detection of node that left the network in reasonable time
                    if (SequenceNumber - FS_Resp_Seq > 1):
                        print("Peer {0} is no longer alive" .format(Successor_1))
                        TCP_msg['Code'] = 4
                        TCP_msg['Successor'] = 1
                        TCP_msg['Port'] = port
                        TCP_Peer_Left = TCPClient(host, port_SS, 5)
                        time.sleep(0.5)
                        TCP_Peer_Left.establish_connection()
                        TCP_Peer_Left.sendData(TCP_msg)
                    if (SequenceNumber - SS_Resp_Seq > 1):
                        print("Peer {0} is no longer alive" .format(Successor_2))
                        TCP_msg['Code'] = 4
                        TCP_msg['Successor'] = 2
                        TCP_msg['Port'] = port
                        TCP_Peer_Left = TCPClient(host, port_FS, 5)
                        time.sleep(0.5)
                        TCP_Peer_Left.establish_connection()
                        TCP_Peer_Left.sendData(TCP_msg)

                    #Send Pings to successors
                    msg['Code'] = 8
                    msg['Sequence Number'] = SequenceNumber
                    msg['Successor'] = 1

                    #7.1.2 Using UDP For Ping Messages
                    UDPsock.sendData(msg, host, port_FS)
                    msg['Successor'] = 2
                    UDPsock.sendData(msg, host, port_SS)

                    SequenceNumber += 1
                    time_PING = time.time()
             
                
                flag, data, addr = UDPsock.readData() #Look out for responses
                
                if flag == False: #If not response then continue while loop
                    continue

                response = pickle.loads(data)
                
                ip, port_ping = addr
                
                #If statements determining type of message and appropriate response
                if (response['Type'] == 'Ping'):
                    if (response['Code'] == 0): #7.1.4:Correct receiving of ping response
                        if(port_ping == port_FS):
                            print("A ping response was received from peer {0}."  .format(Successor_1))
                            FS_Resp_Seq = response['Sequence Number']
                        if(port_ping == port_SS):
                            print("A ping response was received from peer {0}." .format(Successor_2))
                            SS_Resp_Seq = response['Sequence Number']
                    elif (response['Code'] == 8): #7.1.3:Correct receiving of ping request
                        print("A ping request was received from peer {0}." .format(port_ping-address_offset))
                        if(response['Successor'] == 1):
                                port_pre1 = port_ping
                        else:
                                port_pre2 = port_ping
                        msg['Code'] = 0
                        msg['Sequence Number'] = response['Sequence Number']
                        UDPsock.sendData(msg, host, port_ping)
                    elif(response['Code'] == 4): #Resonse form #3
                        SequenceNumber = 1
                        if(response['Successor'] == 2):
                            port_SS = response['PortSS']
                            SuccessorChange['Second'] = True
                        else:
                            port_FS = port_SS
                            port_SS = response['PortFS']
                            SuccessorChange['First'] = True
                            SuccessorChange['Second'] = True
                    elif(response['Code'] == 3 and response['Port'] == port): #Response from graceful departure request
                        TCP_Depart_Pre1.disconnect()
                        TCP_Depart_Pre2.disconnect
                        thread_stop += 1 #Require response from both predecessors for shutdown
                
                UpdateSuccessor(SuccessorChange)

                        
#TCP Server class that threads in the background        
class TCPServer(threading.Thread):

    def __init__(self, host, port):
            threading.Thread.__init__(self)
            self.host = host
            self.port = port
            self.socket = None
            self.startup()
    
    #Create socket
    def startup(self):
        if self.socket is None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #Listen for incoming TCP connections
    def run(self):
        global port_FS, port_SS

        self.socket.bind((self.host, self.port))
        self.socket.listen(1)
        time.sleep(0.2)
   
        while True:          
            try:
                self.conn, self.addr = self.socket.accept()
                
            except Exception:
                print("Connection failed")
                continue 
            
            if(thread_stop == 2): #If peer is leaving, exit while loop
                break

            #If a connection has been established, begin listening on port for incoming data
            process = multiprocessing.Process(target = handle, args = (self.conn, self.addr)) 
            process.daemon = True
            process.start()


    
        self.disconnect()

                           
    def sendData(self, data):
            
        self.socket.sendall(pickle.dumps(data))
        
    def disconnect(self):
        try:
            process.terminate()
            self.socket = None
            self.shutdown(SHUT_RDWR)
            self.close()
        except Exception:
            pass          

#Class for handling TCP Client
class TCPClient():
    
    def __init__(self, host, port, max_attempts):
        self.host = host
        self.port = port
        self.max_attempts = max_attempts
        self.socket = None
        self.connected = False
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    #The client will try "max_attempts" times to connect to a TCP server    
    def establish_connection(self, tries = 0):
    
        if tries == self.max_attempts:
            print("Connection failed")
            self.end = True
            return

        try:
            self.socket.connect((self.host, self.port))
            self.connected = True
        except Exception:
            pass
   
        if self.connected == False:
            self.establish_connection(tries+1)
      
        
                
    def sendData(self, data):
        if self.socket is None:
            self.establish_connection()
        
        time.sleep(1)
        self.socket.sendall(pickle.dumps(data))
            
           
    def disconnect(self):
        try:
            self.socket = None
            self.shutdown(SHUT_RDWR)
            self.close()
        except Exception:
            pass
            
def main(argv=None):

    #Handle wrong number of arguments
    if (len(sys.argv) != 6):
        print('Wrong Number of Arguments')
        return

    #Declare global variables    
    global host, port, port_FS, port_SS, peerID, Successor_1, Successor_2, MSS, DropProb, keystr, str_array, TCP_Server, TCP_Client, TCP_msg, \
                start_time, UDPsock, UDP_FT, thread_stop, TCP_msg, Reliable_UDP_msg, address_offset, FT_offset, ping_delay, port_pre1, port_pre2, TCP_Depart_Pre1, TCP_Depart_Pre2
    
    #Create skeleton messages for TCP and UDP messages
    TCP_msg = {'Type':"Tcp",'Code':True, 'File':True, 'Sequence Number':True, 'Requesting Port':True, 'Responding Port':True, 'Port':True, 'PortFS':True,'PortSS':True, 'Successor':True}
    Reliable_UDP_msg = {'Type':"Udp",'Code':True,'Event':True, 'Time':True, 'Sequence Number':True, 'Bytes':True, 'Ack':True, 'Sending Port':True, 'Payload':True}
    
    #Update variables
    start_time = time.time()
    thread_stop = 0
    ping_delay = 15      
    [peerID, Successor_1, Successor_2, MSS, DropProb] = sys.argv[1:]
    host = "127.0.0.1"
    str_array = ""
    address_offset = 50000
    FT_offset = 300
    MSS = int(MSS)
    port = address_offset  + int(peerID)
    port_FS = address_offset  + int(Successor_1)
    port_SS = address_offset  + int(Successor_2)
    TCP_Sequence_No = 1

    #Clear logs and data file
    f = open("responding_log.txt", "w+")
    f.close()

    f = open("requesting_log.txt", "w+")
    f.close()
    
    f = open("received_file.pdf", "w+")
    f.close()
    
    #UDP sockets for pings
    UDPsock = UDP(host, port, 0.5)
    UDPsock.connect()

    #UDP sockets for file transfer
    UDP_FT = UDP(host,port+FT_offset,0.5)
    UDP_FT.connect()

    #Start threads and processes
    Ping_Thread = ping()
    TCP_Client = TCPClient(host, port_FS, 5)

    TCP_Server = TCPServer(host, port)
    TCP_Server.start()
    
    time.sleep(0.5)
    TCP_Client.establish_connection(0)
    
    Ping_Thread.start()
    
    while True:
        keystr = input() #Read keyboard inputs

        #7.3.1: Sending the departure message to the predecessors
        if (keystr == "quit"): #Begin graceful departure procesdure
            TCP_msg['Code'] = 3
            TCP_msg['Port'] = port
            TCP_msg['PortFS'] = port_FS
            TCP_msg['PortSS'] = port_SS

            #7.3.2: Using TCP for sending departure message
            TCP_Depart_Pre1 = TCPClient(host, port_pre1, 5)
            TCP_Depart_Pre2 = TCPClient(host, port_pre2, 5)
            time.sleep(0.5)
            TCP_Depart_Pre1.establish_connection()
            TCP_Depart_Pre2.establish_connection()
            TCP_Depart_Pre1.sendData(TCP_msg)
            TCP_Depart_Pre2.sendData(TCP_msg)

            break

        try:
            str_array = keystr.split()
        except Exception:
            print("Please enter valid request ")

        #7.2.1: Initializing and sending the file request to the successor 
        if str_array:
            if (str_array[0] == "request"):
                if(len(str_array[1]) == 4 and str_array[1].isdigit()): #Begin file transfer procedure
                    filename = str_array[1]
                    hash_no = int(filename)%256
                    TCP_msg['File'] = filename
                    TCP_msg['Sequence Number'] = TCP_Sequence_No
                    TCP_msg['Requesting Port'] = port
                    if((hash_no > int(peerID) >= int(Successor_1)) and ((hash_no - int(peerID)) > (256+int(Successor_1) - hash_no))):
                        TCP_msg['Code'] = 1
                    else:
                        TCP_msg['Code'] = 0
                    TCP_Client.sendData(TCP_msg)
                    print("File request message for {0} has been sent to my successor." .format(filename))
                    str_array = ""
                    TCP_Sequence_No += 1
                else:
                    print("Please enter 4 digit filename")
            else:
                print("Please enter valid request")
            
        if(thread_stop == 2): #Peer leaving
            break

    TCP_Server.disconnect()
    TCP_Client.disconnect()



           

if __name__ == "__main__":
    sys.exit(main())        
                   

