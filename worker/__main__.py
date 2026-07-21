import asyncio
import sys

from worker.app import main
from worker.cli import parse_args

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    asyncio.run(main(args))
