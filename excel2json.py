import excel2json
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-x", help="Excel file")

    args = parser.parse_args()

    if args.x:
    	excel2json.convert_from_file(args.x)

