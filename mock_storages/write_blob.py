#!/usr/bin/env python3
"""Write one blob into the source cell's filestore and print its content-addressed key.
Used by `make demo` to stage a real blob behind a file row inserted mid-stream."""
import hashlib
import sys

import yaml

from dependencies import FLEET
from generate_data import write_blob


def main() -> None:
    contents = sys.argv[1] + "\n"
    with open(FLEET) as f:
        blobs = yaml.safe_load(f)["cells"]["source"]["blobs"]
    key = hashlib.sha1(contents.encode()).hexdigest()
    write_blob(blobs["filestore"], key, contents)
    print(key)


if __name__ == "__main__":
    main()
