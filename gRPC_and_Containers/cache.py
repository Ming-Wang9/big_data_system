import os
import grpc
import property_lookup_pb2
import property_lookup_pb2_grpc
import flask
from flask import Flask, jsonify, request
import time
from collections import OrderedDict

app = Flask("p2")

PROJECT = os.environ.get("PROJECT", "p2")
DATASET_SERVERS = [f"{PROJECT}-dataset-1:5000", f"{PROJECT}-dataset-2:5000"]

last_used = 0
channels = {}
CACHE_SIZE = 3
cache = OrderedDict()

def dataset_stub():
    global last_used
    server = DATASET_SERVERS[last_used]
    last_used = (last_used + 1) % len(DATASET_SERVERS)

    if server not in channels:
        channels[server] = grpc.insecure_channel(server)

    return property_lookup_pb2_grpc.PropertyLookupStub(channels[server]), server

def get_from_cache(zipcode):
    if zipcode in cache:
        addresses = cache.pop(zipcode)
        cache[zipcode] = addresses
        return addresses
    return None

def add_to_cache(zipcode, addresses):
    if zipcode in cache:
        cache.pop(zipcode)
    cache[zipcode] = addresses[:8]  
    if len(cache) > CACHE_SIZE:
        cache.popitem(last=False)

@app.route("/lookup/<zipcode>")
def lookup(zipcode):
    try:
        zipcode = int(zipcode)
        limit = request.args.get("limit", default=4, type=int)

        if limit <= 8:
            cached = get_from_cache(zipcode)
            if cached:
                return jsonify({"addrs": cached[:limit], "source": "cache", "error": None})

        error = None
        for _ in range(5):
            try:
                stub, server = dataset_stub()
                response = stub.LookupByZip(property_lookup_pb2.Request(zip=zipcode, limit=limit))
                addrs = list(response.address)
                
                if addrs:
                    add_to_cache(zipcode, addrs[:8])
                
                return jsonify({
                    "addrs": addrs[:limit],
                    "source": "1" if "1" in server else "2",
                    "error": None
                })
                
            except grpc.RpcError as e:
                error = e
                if server in channels:
                    del channels[server]  
                time.sleep(0.1)

        cached = get_from_cache(zipcode)
        if cached and limit <= 8:
            return jsonify({"addrs": cached[:limit], "source": "cache", "error": None})
        else:
            return jsonify({
                "addrs": [],
                "source": None,
                "error": f"All servers down: {error}"
            })

    except ValueError:
        return jsonify({"addrs": [], "source": None, "error": "Invalid zipcode"})

def main():
    app.run("0.0.0.0", port=8080, debug=False, threaded=False)

if __name__ == "__main__":
    main()