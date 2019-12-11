import json, sys

for arg in sys.argv[1:]:
    with open(sys.argv[1]) as input:
        s = json.dumps(json.load(input), indent=2)
        print s