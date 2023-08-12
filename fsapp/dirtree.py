import os

def make_tree(path):
    lst = []
    
    for file in os.scandir(path=path):
        lst.extend([{"filename": file.name,
                     "path": os.path.dirname(file.path),
                     "type": "D" if file.is_dir() else "F"}])

    return lst