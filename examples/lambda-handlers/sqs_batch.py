import json, sys
def _process(record):
    body = json.loads(record["body"])          # invalid JSON -> failure
    if int(body["value"]) < 0:                 # negative value -> failure
        raise ValueError("negative value")
def handler(event, context=None):
    failures = []
    for r in event["Records"]:
        try:
            _process(r)
        except Exception:
            failures.append({"itemIdentifier": r["messageId"]})
    return {"batchItemFailures": failures}
if __name__ == "__main__":
    print(json.dumps(handler(json.load(open(sys.argv[1])))))
