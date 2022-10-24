import pathlib
import sys

# It's not possible to use the data_files in setup. It's deprecated
# So, I write a python wrapper.
def main():
    import subprocess

    root = pathlib.Path(__file__).parent / "build-conda-vdf-env"
    args = [str(root)] + sys.argv[1:]
    subprocess.run(args)
