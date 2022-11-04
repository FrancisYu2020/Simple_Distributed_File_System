import threading
import socket
import sys
import time

PORT = 20001

def get_host(number):
    '''
    param number: the illinois vm number,ranges from 01 to 10
    return: host string
    '''
    return "fa22-cs425-22%02d.cs.illinois.edu" % number
    
class Client:
    def __init__(self,args):
        self.args = args
        # a map to record the search time cost in each file. key: file name, value: time cost
        self.time_cost = {}
        self.total_lines = 0
        
    def client_program(self, command, host, port):
        '''
        get the data from server and print the required information
        param command: the gerp command
        param host: host name
        param port: port number
        return: None
        '''
        try:
            time_start = time.time()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host,port))
                s.sendall(command.encode('utf-8'))
                data = b''
                deadline = time.time() + 5
                while True:
                    bytes_received = s.recv(4096)
                    data += bytes_received
                    if bytes_received:
                        s.send(b'FINISH')
                        break
                    if time.time() > deadline:
                        print("[ERROR]", "connection timeout exceeded 5 seconds")
                        return                
            time_end = time.time()
            time_cost = time_end - time_start

            received_data = data.decode('utf-8').strip("\n")
            filename, line_cnt = received_data.split(":")
            self.time_cost[filename] = time_cost
            self.total_lines += int(line_cnt)
            print("[RESULT]", "host is %s, " % host, end = "")
            print("{filename} has {line_count} matched lines, ".format(filename = filename, line_count = line_cnt), end = "")
            print("time cost is %f seconds" % time_cost, end = "\n")
        except:
            raise Exception("error connect to server")

    def query(self):
        '''
        send the query request and get data in 10 virtual machines
        return: None
        '''
        time_start = time.time()
        threads = []
        # change to 11 for grep all
        for i in range(1,6):
            host = get_host(i)
            t = threading.Thread(target = self.client_program, args = (self.args, host, PORT))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()

        time_end = time.time()
        self.time_cost["total"] = time_end - time_start
        print("Total lines are %d" % self.total_lines)

if __name__=="__main__":
    if len(sys.argv) < 4:
        print("Error: Input arguments are not sufficient, you should use 'grep_client -c/-Ec [pattern] Datanode/Namenode'")
    else:
        s = ' '.join(sys.argv[1:])
        c = Client(s)
        c.query()
