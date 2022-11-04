import socket
import re
import os

HOST = socket.gethostname()
PORT = 20001


def grep(request):
    """
    executes `grep -c pattern`/ `grep -Ec pattern` on all *.log files in the same directory
    param request: the grep request from user
    return: line count message or error message
    """
    args = request.split()
    print(args)
    if len(args) != 4 or args[0] != 'grep' or (args[1] != '-c' and args[1] != '-Ec'):
        return "Wrong format! Correct format: `grep -c pattern` or `grep -Ec pattern`"
    res = ''
    pattern = args[2]
    if pattern[0] == '\"' and pattern[-1] == '\"':
        pattern = pattern[1:-1]
    print("updated pattern is: ", pattern)
    # put all files under current directory in a list
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    # filter to all *.log files
    log_files = [f for f in files if f.endswith(args[3] + '.log')]
    # iterate all log files
    for file in log_files:
        with open(file, encoding="utf-8") as f:
            c_cnt = ec_cnt = 0  # c_cnt is for exact pattern matching / ec_cnt is for regex pattern search
            for line in f:
                if line.count(pattern) > 0:
                    c_cnt += 1
                if len(re.findall(pattern, line)) > 0:
                    ec_cnt += 1
            if args[1] == '-c':
                res += "{file}:{line_count}\n".format(file=file, line_count=c_cnt)
            elif args[1] == '-Ec':
                res += "{file}:{line_count}\n".format(file=file, line_count=ec_cnt)
            else:
                return "Only grep -c / -Ec options are supported!"
    return res


def server_program():
    """
    server program that listens on (HOST, PORT)
    takes in the request, executes grep(), and return grep()'s result to requester
    return: None
    """
    print("[INFO]: MP1 server started")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen()

    while True:
        conn, addr = s.accept()
        print("[INFO]: connection from: " + str(addr))
        while True:
            request = conn.recv(4096).decode("utf-8")
            if request:
                if request == 'FINISH':
                    print("[INFO]: connection from: " + str(addr) + "ended!")
                    break
                print("[INFO] client request: " + str(request))
                result = grep(request)
                print("[INFO]: result is: ", result)
                conn.send(result.encode("utf-8"))
    conn.close()


if __name__ == '__main__':
    server_program()
