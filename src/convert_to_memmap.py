import pathlib
import argparse
import fastavro
import numpy as np


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int)
    parser.add_argument("--target-path")
    parser.add_argument("--target-attr")
    parser.add_argument("--output-path")
    return  parser.parse_args()


def get_array_size(path, target):
    for p in pathlib.Path(path).glob("**/out-*.avro"):
        with p.open(mode="rb") as fo:
            for record in fastavro.reader(fo):
                return len(record[target])
    raise Exception(path + " is not found")


def main():
    args = get_args()
    array_size = get_array_size(args.target_path, args.target_attr)
    shape = (args.rows, array_size)
    print(shape)
    fp = np.memmap(args.output_path, dtype='float32', mode='w+', shape=shape)

    for p in pathlib.Path(args.target_path).glob("**/out-*.avro"):
        with p.open(mode="rb") as fo:
            records = [r[args.target_attr] for r in fastavro.reader(fo)]
            fp[:] = records[:]


if __name__ == "__main__":
    main()
