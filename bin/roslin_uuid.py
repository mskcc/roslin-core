#!/usr/bin/env python

import uuid
import hashlib
import base64
import argparse

def generatePrismUUID(uuidLength,altChar):
	uuidHex=uuid.uuid4().hex
	uuidHash=hashlib.sha256(uuidHex)
	return base64.b64encode(uuidHash.digest(),altChar)[:uuidLength]

def main():
    "main function"

    parser = argparse.ArgumentParser(description='uuid')

    parser.add_argument(
        "--length",
        action="store",
        help="The length of the uuid",
        type=int,
        default=12
    )

    parser.add_argument(
        "--altChar",
        action="store",
        help="The alt char of the uuid",
        type=str,
        default="-_"
    )

    params = parser.parse_args()

    print generatePrismUUID(params.length,params.altChar)

if __name__ == "__main__":

    main()