

from modereddit import ModeredditCore, VERSION

import argparse

def get_argument_parser():
    """
    Create a parser with all the core options for the user to
    configure the input, processing and output of the word count.
    Returns:
        parser: Comtaining all the configuration to be displayed
    """
    parser = argparse.ArgumentParser(
        description='Count the number of words in the files on a folder',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
EXAMPLE USAGE:
                modereddit.py --model MODEL_FILE 
        """)
    parser.add_argument("--model", type=str,
        help="Provide a path to the trained model to use")

    return parser

def main(args):
    """
    Deploys a ModeredditCore on the arguments that have been given through the
    command line interface.
    Raises:
        InvalidColumnException: When a column provided in the options is invalid
        PathNotValidException: When path provided does not exist or not valid
    """
    modereddit = ModeredditCore(
                    model=args.model
                    )

    modereddit.start_streaming_server()


