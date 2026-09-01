import json, sys
def handler(event, context=None):
    # S3 Object Lambda transforms the object content before returning it.
    return {"transformed_content": event["content"].upper()}
if __name__ == "__main__":
    print(json.dumps(handler(json.load(open(sys.argv[1])))))
