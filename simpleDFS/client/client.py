import zerorpc

class Client:
    def __init__(self):
        pass

    def printCommands(self):
        commands = '''
        ***********************  Commands  *************************
        1. put <localfilename> <sdfsfilename>
        2. get <sdfsfilename> <localfilename>
        3. delete <sdfsfilename>
        4. ls <sdfsfilename>
        5. store
        6. get-versions <sdfsfilename> <numversions> <localfilename>
        7. exit
        ************************************************************
        '''
        print(commands)
        return

    def run(self):
        self.printCommands()
        while True:
            command = input("Please input command:\n")
            arg = command.split(" ")
            if len(arg) == 3:
                if arg != "put" or arg != "get" or arg != "delete":
                    print("Invalid Input!")
                    continue
                if arg == "put":
                    pass

    def put(self, local_filename, sdfs_filename):
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        data_node_address = c.put_file(sdfs_filename)
        c.close()
        content = open(local_filename).read()
        for address in data_node_address:
            c = zerorpc.Client()
            c.connect("tcp://" + address + ":4242")
            c.put_file(sdfs_filename, content)
            c.close()
    
    def get(self, sdfs_filename, local_filename):
        # get address
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        data_node_address = c.get_file(sdfs_filename)
        c.close()
        # write to local
        c = zerorpc.Client()
        c.connect("tcp://" + data_node_address + ":4242")
        content = c.get_file(sdfs_filename)
        c.close()
        f = open(local_filename, 'w')
        f.write(content)
    
    def delete(self, sdfs_filename):
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        c.delete_file(sdfs_filename)
        c.close()
    
    def ls(self, sdfs_filename):
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        print(c.ls(sdfs_filename))
        c.close()

    def store(self, data_node_id):
        c = zerorpc.Client()
        c.connect("tcp://127.0.0.1:4241")
        c.store(data_node_id)
        c.close()

# test part
c = Client()
c.put("client.py", "test")
c.get("test", "data_node_cpy")
c.ls("test")
c.delete("test")
