import json


def save(filename, obj):
    # save the data to prevent having to look for it again
    with open(f"{filename}", "w") as f:
        json.dump(obj, f)

def filter_default(filter_func, iter: list, default_value):
    # do a filter, and if value wasn't found, send default.
    # only first value is returned
    filtered_iter = filter(filter_func, iter)
    return filtered_iter[0] if len(filtered_iter) > 0 else default_value
