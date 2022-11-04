import zerorpc
c = zerorpc.Client(timeout=5)
c.connect("tcp://fa22-cs425-2201.cs.illinois.edu:4242")
c.put_file("store/test1", open("test.pdf", "rb").read(), [])
c.close()
while(True):
    c = zerorpc.Client(timeout=1)
    c.connect("tcp://fa22-cs425-2201.cs.illinois.edu:4242")
    content = c.get_file("test1")
    c.close()