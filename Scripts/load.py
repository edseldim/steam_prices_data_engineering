import json

def save_data_checkpoint(filename, obj):
    # save the data to prevent having to look for it again
    with open(f"Data/{filename}","w") as f:
        json.dump(obj,f)