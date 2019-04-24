#!/bin/python3

from modereddit.main import get_argument_parser
from modereddit.main import main

if __name__ == "__main__":
    parser = get_argument_parser()
    args = parser.parse_args()
    main(args)
