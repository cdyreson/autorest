import json

with open('generatedfiles/demooutput.txt', 'r') as file:
    uglyjson = file.read()
parsed = json.loads(uglyjson)
prettyjson = json.dumps(parsed, indent=2)
f = open('generatedfiles/demooutput.txt', 'r+')
f.truncate(0)
f.write(prettyjson)
f.close()
