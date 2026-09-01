import json, sys
def handler(event, context=None):
    return {"area": int(event["length"]) * int(event["width"])}
if __name__ == "__main__":
    print(json.dumps(handler(json.load(open(sys.argv[1])))))
