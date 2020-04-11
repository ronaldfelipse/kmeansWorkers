import zmq
import random
import math
import json

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")


def CreateDataSet(Dimensiones):
    DataSet = []
    
    Cont = 0
    while Cont != 100:
        
        dotTemp = []
        for i in range(Dimensiones):
            Atribute = 0
            
            if int(Dimensiones) == 2:
                Atribute = random.randrange(600)
            else:
                Atribute = random.randrange(99999)
            
            dotTemp.append(Atribute)
        DataSet.append(dotTemp)
        Cont = Cont + 1
    return DataSet

def Createcentroides(K,Dimensiones):
    
    Centroides = []
    
    for i in range(K):
        
         Centroidetemp = []
        
         for i in range(Dimensiones):

            Atribute = 0
            
            if int(Dimensiones) == 2:
                Atribute = random.randrange(600)
            else:
                Atribute = random.randrange(99999)
            
            Centroidetemp.append(Atribute)
         Centroides.append(Centroidetemp)
        
    return Centroides

def SendsPoints(workers,setdata,Centroides):
    
    Pointer = 0
    TamPoints = 10 ####
    workCount = 1
    
    DotsToSend = []
    
    while Pointer < len(setdata):
        
        DotsToSend.append(setdata[Pointer])
        
        if len(DotsToSend) == TamPoints:
            workers.send_multipart([Strencode(json.dumps(DotsToSend)),Strencode(json.dumps(Centroides)),Strencode(workCount)])
            DotsToSend = []
            workCount = workCount +1
        
        Pointer = Pointer + 1

def CalculateDist(Point1,Point2):
    
    Sum = 0
    
    for i in range(len(Point1)):
        Sum = Sum + ( Point1[i] - Point2[i] ) ** 2
        
    return math.sqrt(Sum)

def  Evaluatemovement(firstEstate,secondstate):
    
    ChangeValue = 5
    
    for i in range(len(firstEstate)):
        if CalculateDist(firstEstate[i],secondstate[i]) > ChangeValue:
            return True
    return False        
    

def Main():
    
    Dimensiones = 2 ####
    K = 4 ####
    
    DataSet = CreateDataSet(Dimensiones)
    print(DataSet)
    Centroides = Createcentroides(K,Dimensiones)
    print("-----------------------------")
    print("First Centroides: ")
    print(Centroides)
    
    
    context = zmq.Context()
    
    # socket with workers
    workers = context.socket(zmq.PUSH)
    workers.bind("tcp://*:5557")
    
    # socket with sink
    sinkSend = context.socket(zmq.PUSH)
    sinkSend.connect("tcp://localhost:5558")
    
    sinkRecive = context.socket(zmq.PULL)
    sinkRecive.connect("tcp://localhost:5556")
    
    print("Press enter when workers are ready...")
    _ = input()
    print("sending tasks to workers")
    
    sinkSend.send_multipart([Strencode(len(DataSet))])
    
    EndsKmeans = False
    
    while not EndsKmeans:
        
        sinkSend.send_multipart([Strencode(json.dumps(Centroides))])
        SendsPoints(workers,DataSet,Centroides)    
        Newcentroids = sinkRecive.recv_multipart()
        Newcentroids = json.loads(Bdecode(Newcentroids[0]))
        
        if Evaluatemovement(Centroides,Newcentroids):
            Centroides = Newcentroids
        else:
            print("Final Centroides: ")
            print(Centroides)
            EndsKmeans = True
            
    
    

Main()


