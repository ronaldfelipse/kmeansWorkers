import zmq
import json
import math

def Strencode(strToEncode):
    return str(strToEncode).encode("utf-8")

def Bdecode(bToEncode):
    return bToEncode.decode("utf-8")

def CalculateDist(Point1,Point2):
    
    Sum = 0
    
    for i in range(len(Point1)):
        Sum = Sum + ( Point1[i] - Point2[i] ) ** 2
        
    return math.sqrt(Sum)
    

def CalculateCentroide(Point,Centroids):
    
    DistMenor = 0
    CentroiID = 0
    
    for i in range(len(Centroids)):
        if i == 0 :
             DistMenor = CalculateDist(Point,Centroids[i])
        else:
             DistTemp = CalculateDist(Point,Centroids[i])
             if DistTemp < DistMenor :
                 CentroiID = i
                 DistMenor = DistTemp
            
    return CentroiID
         
def SumVect(Vect1,Vect2):
    
    VecTSum = []
    
    for i in range(len(Vect1)):
        Val = Vect1[i] + Vect2[i]
        VecTSum.append(Val)
        
    return VecTSum

def Main():

    context = zmq.Context()
    
    work = context.socket(zmq.PULL)
    work.connect("tcp://localhost:5557")
    
    # Socket to send messages to
    sink = context.socket(zmq.PUSH)
    sink.connect("tcp://localhost:5559")
    
    # Process tasks forever
    while True:
        s = work.recv_multipart()
        
        Points = Bdecode(s[0])
        Centrois = Bdecode(s[1])
        WorkId = Bdecode(s[2])
        
        print("Procesing work : "+WorkId)
        
        Points = json.loads(Points)
        Centrois = json.loads(Centrois)
        
        dicc = {}
        
        BaseVector = []
        for i in range(len(Points[0])):
            BaseVector.append(0)
            
        for j in range(len(Centrois)):
            dicc[j] = {}
            dicc[j]["Sumatoria"] = BaseVector
            dicc[j]["Cant"] = 0
            
        for k in range(len(Points)):
            CentroID = CalculateCentroide(Points[k],Centrois)
            dicc[CentroID]["Cant"] = dicc[CentroID]["Cant"] + 1
            dicc[CentroID]["Sumatoria"] = SumVect(dicc[CentroID]["Sumatoria"],Points[k])
            

        # Send results to sink
        sink.send_multipart([Strencode(json.dumps(dicc))])

Main()