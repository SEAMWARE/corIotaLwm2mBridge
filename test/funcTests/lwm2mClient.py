#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# lwm2mClient.py - an LwM2M client for the tests: CoAP over UDP, by hand (no CoAP library on the CI image)
#
#   lwm2mClient.py --server 127.0.0.1:15683 --ep <name> --control <port> --log <file>
#                  [--res 3303/0/5700=21.5 ...] [--readonly 3303/0/5700 ...] [--fail-execute 3/0/4 ...]
#
# It registers (POST /rd?ep=<name>&lt=60&lwm2m=1.1&b=U, its objects from its resources) and then answers the
# server: a Read or an Observe (GET, text/plain), a Write (PUT; a read-only resource 4.05), an Execute (POST;
# --fail-execute 4.05). What the server asked goes to the log, a line each:
#
#   REGISTERED rd/<id>  |  OBSERVE <path>  |  READ <path>  |  WRITE <path> <value>  |  EXECUTE <path> [<arguments>]
#
# The test drives it over HTTP on the control port:
#
#   GET /set?path=<path>&value=<value>   the resource changes on the device - its observers notified
#   GET /update  /deregister  /register  the registration updated, removed, made again
#
import argparse
import random
import socket
import struct
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

CON, NON, ACK, RST = 0, 1, 2, 3
GET, POST, PUT, DELETE = 1, 2, 3, 4
CREATED, DELETED, CHANGED, CONTENT = 65, 66, 68, 69
NOT_FOUND, METHOD_NOT_ALLOWED = 132, 133
OBSERVE, LOCATION_PATH, URI_PATH, CONTENT_FORMAT, URI_QUERY, ACCEPT = 6, 8, 11, 12, 15, 17


def encode(mtype, code, mid, token, options, payload=b""):
    out = bytes([0x40 | (mtype << 4) | len(token), code]) + struct.pack(">H", mid) + token
    prev = 0
    for number, value in sorted(options, key=lambda o: o[0]):
        delta, length = number - prev, len(value)
        head, ext = 0, b""
        for v, shift in ((delta, 4), (length, 0)):
            if v < 13:
                head |= v << shift
            elif v < 269:
                head |= 13 << shift
                ext += bytes([v - 13])
            else:
                head |= 14 << shift
                ext += struct.pack(">H", v - 269)
        out += bytes([head]) + ext + value
        prev = number
    if payload:
        out += b"\xff" + payload
    return out


def decode(data):
    mtype, tkl, code = (data[0] >> 4) & 3, data[0] & 0xF, data[1]
    mid = struct.unpack(">H", data[2:4])[0]
    token = data[4:4 + tkl]
    i, number, options, payload = 4 + tkl, 0, [], b""
    while i < len(data):
        if data[i] == 0xFF:
            payload = data[i + 1:]
            break
        delta, length = data[i] >> 4, data[i] & 0xF
        i += 1
        if delta == 13:
            delta, i = 13 + data[i], i + 1
        elif delta == 14:
            delta, i = 269 + struct.unpack(">H", data[i:i + 2])[0], i + 2
        if length == 13:
            length, i = 13 + data[i], i + 1
        elif length == 14:
            length, i = 269 + struct.unpack(">H", data[i:i + 2])[0], i + 2
        number += delta
        options.append((number, data[i:i + length]))
        i += length
    return mtype, code, mid, token, options, payload


def uint(v):
    return b"" if v == 0 else v.to_bytes((v.bit_length() + 7) // 8, "big")


class Client:
    def __init__(self, a):
        host, port = a.server.rsplit(":", 1)
        self.server = (host, int(port))
        self.ep = a.ep
        self.res = dict(r.split("=", 1) for r in a.res)
        self.readonly = set(a.readonly)
        self.failExecute = set(a.fail_execute)
        self.observers = {}                            # path -> token
        self.seq = 2
        self.mid = random.randint(0, 0xFFFF)
        self.location = None
        self.lock = threading.Lock()
        self.log = open(a.log, "a", buffering=1)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.pending = {}                              # mid -> event, response

    def nextMid(self):
        self.mid = (self.mid + 1) & 0xFFFF
        return self.mid

    def request(self, code, path, query=(), payload=b"", fmt=None):
        mid = self.nextMid()
        options = [(URI_PATH, p.encode()) for p in path.split("/") if p] + [(URI_QUERY, q.encode()) for q in query]
        if fmt is not None:
            options.append((CONTENT_FORMAT, uint(fmt)))
        ev = threading.Event()
        self.pending[mid] = [ev, None]
        msg = encode(CON, code, mid, bytes([random.randint(0, 255) for _ in range(4)]), options, payload)
        for _ in range(4):
            self.sock.sendto(msg, self.server)
            if ev.wait(2):
                break
        return self.pending.pop(mid)[1]

    def register(self):
        objects = sorted({"/".join(p.split("/")[:2]) for p in self.res})
        links = ",".join("</%s>" % o for o in objects).encode()
        with self.lock:
            self.observers = {}                        # before: the server observes as soon as it has answered
        r = self.request(POST, "rd", ("ep=" + self.ep, "lt=60", "lwm2m=1.1", "b=U"), links, 40)
        if r is not None:
            self.location = "/".join(v.decode() for n, v in r[4] if n == LOCATION_PATH)
            self.log.write("REGISTERED %s\n" % self.location)

    def notify(self, path):
        token = self.observers.get(path)
        if token is None:
            return
        self.seq += 1
        options = [(OBSERVE, uint(self.seq)), (CONTENT_FORMAT, b"")]
        self.sock.sendto(encode(NON, CONTENT, self.nextMid(), token, options, self.res[path].encode()), self.server)

    def answer(self, mid, token, code, options=(), payload=b""):
        self.sock.sendto(encode(ACK, code, mid, token, list(options), payload), self.server)

    def serve(self):
        while True:
            data, addr = self.sock.recvfrom(2048)
            mtype, code, mid, token, options, payload = decode(data)
            if code == 0 or code >= 64:                # an ACK of ours, a response
                if mid in self.pending:
                    self.pending[mid][1] = (mtype, code, mid, token, options, payload)
                    self.pending[mid][0].set()
                continue
            path = "/".join(v.decode() for n, v in options if n == URI_PATH)
            with self.lock:
                if code == GET:
                    if path not in self.res:
                        self.answer(mid, token, NOT_FOUND)
                        continue
                    observe = [v for n, v in options if n == OBSERVE]
                    if observe and observe[0] == b"":
                        self.observers[path] = token
                        self.log.write("OBSERVE %s\n" % path)
                        self.answer(mid, token, CONTENT, [(OBSERVE, uint(self.seq)), (CONTENT_FORMAT, b"")], self.res[path].encode())
                    else:
                        self.log.write("READ %s\n" % path)
                        self.answer(mid, token, CONTENT, [(CONTENT_FORMAT, b"")], self.res[path].encode())
                elif code == PUT:
                    value = payload.decode()
                    self.log.write("WRITE %s %s\n" % (path, value))
                    if path in self.readonly or path not in self.res:
                        self.answer(mid, token, METHOD_NOT_ALLOWED if path in self.readonly else NOT_FOUND)
                    else:
                        self.res[path] = value
                        self.answer(mid, token, CHANGED)
                        self.notify(path)
                elif code == POST:
                    args = payload.decode()
                    self.log.write("EXECUTE %s%s\n" % (path, (" " + args) if args else ""))
                    self.answer(mid, token, METHOD_NOT_ALLOWED if path in self.failExecute else CHANGED)
                else:
                    self.answer(mid, token, METHOD_NOT_ALLOWED)


def control(client, port):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = dict(urllib.parse.parse_qsl(u.query))
            if u.path == "/set":
                with client.lock:
                    client.res[q["path"]] = q["value"]
                    client.notify(q["path"])
            elif u.path == "/update":
                client.request(POST, client.location)
            elif u.path == "/deregister":
                client.request(DELETE, client.location)
            elif u.path == "/register":
                client.register()
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    HTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--server", required=True)
    p.add_argument("--ep", required=True)
    p.add_argument("--control", type=int, required=True)
    p.add_argument("--log", required=True)
    p.add_argument("--res", action="append", default=[])
    p.add_argument("--readonly", action="append", default=[])
    p.add_argument("--fail-execute", action="append", default=[])
    a = p.parse_args()
    c = Client(a)
    threading.Thread(target=c.serve, daemon=True).start()
    c.register()
    control(c, a.control)
