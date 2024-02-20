import json

def save(filename, obj):
    # save the data to prevent having to look for it again
    with open(f"{filename}", "w") as f:
        json.dump(obj, f)
