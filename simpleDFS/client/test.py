import zerorpc

while(True):
    c = zerorpc.Client(timeout=1)
    c.connect("tcp://fa22-cs425-2205.cs.illinois.edu:4242")
    content = c.get_file("test1")
    c.close()