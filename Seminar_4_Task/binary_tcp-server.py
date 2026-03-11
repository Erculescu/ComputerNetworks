import socket
import threading
import collections
import pickle
import io

HOST = "127.0.0.1"  
PORT = 3333  

is_running = True
BUFFER_SIZE = 8

class Response:
    def __init__(self, payload):
        self.payload = payload

class Request:
    def __init__(self, command, key=None, resource=None):
        self.command = command
        self.key = key
        self.resource = resource

class State:
    def __init__(self):
        self.resources = {}
        self.lock = threading.Lock()
    def add(self, key, resource):
        with self.lock:
            self.resources[key] = resource
            return "OK - record add"
    def remove(self, key):
        with self.lock:
            if key in self.resources:
                del self.resources[key]
                return "OK value deleted"
            return "ERROR invalid key"
    def get(self, key):
        with self.lock:
            if key in self.resources:
                return f"DATA {self.resources[key]}"
            return "ERROR invalid key"
    def list(self):
        with self.lock:
            items = [f"{k}={v}" for k, v in self.resources.items()]
            return "DATA|" + ",".join(items) if items else "DATA|"
    def count(self):
        with self.lock:
            return f"DATA {len(self.resources)}"
    def clear(self):
        with self.lock:
            self.resources.clear()
            return "all data deleted"
    def update(self, key, new_resource):
        with self.lock:
            if key in self.resources:
                self.resources[key] = new_resource
                return "Data updated"
            return "ERROR invalid key"
    def pop(self, key):
        with self.lock:
            if key in self.resources:
                val = self.resources.pop(key)
                return f"DATA {val}"
            return "ERROR invalid key"

state = State()

def handle_response(payload_str):
    stream = io.BytesIO()
    pickle.dump(Response(payload_str), stream)
    serialized_payload = stream.getvalue()
    payload_length = len(serialized_payload) + 1
    
    # Handle lengths > 255 gracefully by sending an error message instead
    try:
        length_byte = payload_length.to_bytes(1, byteorder='big')
    except OverflowError:
        error_stream = io.BytesIO()
        pickle.dump(Response("ERROR response too large"), error_stream)
        serialized_payload = error_stream.getvalue()
        payload_length = len(serialized_payload) + 1
        length_byte = payload_length.to_bytes(1, byteorder='big')
        
    return length_byte + serialized_payload

def process_command(data):
    if len(data) < 2:
        return handle_response('ERROR invalid command format')
    
    payload = data[1:]
    stream = io.BytesIO(payload)  
    try:
        request = pickle.load(stream)
    except Exception as e:
        return handle_response('ERROR invalid command format')
        
    payload_response = 'ERROR unknown command'
    cmd = str(request.command).upper() if hasattr(request, 'command') and request.command else ""
    
    if cmd == 'ADD':
        if hasattr(request, 'key') and hasattr(request, 'resource'):
            payload_response = state.add(request.key, request.resource)
    elif cmd == 'REMOVE':
        if hasattr(request, 'key'):
            payload_response = state.remove(request.key)
    elif cmd == 'GET':
        if hasattr(request, 'key'):
            payload_response = state.get(request.key)
    elif cmd == 'LIST':
        payload_response = state.list()
    elif cmd == 'COUNT':
        payload_response = state.count()
    elif cmd == 'CLEAR':
        payload_response = state.clear()
    elif cmd == 'UPDATE':
        if hasattr(request, 'key') and hasattr(request, 'resource'):
            payload_response = state.update(request.key, request.resource)
    elif cmd == 'POP':
        if hasattr(request, 'key'):
            payload_response = state.pop(request.key)
    elif cmd == 'QUIT':
        return b'QUIT'

    return handle_response(payload_response)

def handle_client(client):
    with client:
        while True:
            try:
                data = client.recv(BUFFER_SIZE)
                if not data:
                    break
                    
                message_length = data[0]
                full_data = data
                remaining = message_length - len(data)
                
                while remaining > 0:
                    chunk = client.recv(min(remaining, BUFFER_SIZE))
                    if not chunk:
                        break
                    full_data += chunk
                    remaining -= len(chunk)
                    
                response = process_command(full_data)
                if response == b'QUIT':
                    break
                client.sendall(response)
            except Exception as e:
                try:
                    client.sendall(handle_response(f"Error: {str(e)}"))
                except:
                    pass
                break

def accept_connections(server):
    while is_running:
        try:
            client, addr = server.accept()
            print(f"{addr} has connected")
            client_thread = threading.Thread(target=handle_client, args=(client,))
            client_thread.start()
        except Exception:
            break

def main():
    server = None
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((HOST, PORT))
        server.listen()
        print(f"Listening on {HOST}:{PORT}")
        accept_thread = threading.Thread(target=accept_connections, args=(server,))
        accept_thread.start()
        accept_thread.join()
    except BaseException as err:
        print(err)
    finally:
        global is_running
        is_running = False
        if server:
            server.close()

if __name__ == '__main__':
    main()