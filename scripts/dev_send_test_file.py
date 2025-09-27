import argparse, shutil, os, pathlib

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, default=3)
    args = parser.parse_args()
    src = pathlib.Path(__file__).resolve().parents[1] / "data" / "raw" / "xml_drop" / "retail_txn_0001.xml"
    dst = pathlib.Path(__file__).resolve().parents[1] / "data" / "raw" / "xml_drop" / f"retail_txn_{args.id:04d}.xml"
    shutil.copyfile(src, dst)
    print(f"Copied {src.name} -> {dst.name}")

if __name__ == "__main__":
    main()
