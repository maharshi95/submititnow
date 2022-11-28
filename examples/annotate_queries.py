import argparse
import time
import random


def main(args):
    print('This program will pretend to annotate queries of a dataset.')
    time.sleep(random.randint(1, 10))
    print('Model:', args.model)
    print('Dataset:', args.dataset, args.fold)
    

def add_arguments(parser = None):
    parser = parser or argparse.ArgumentParser()
    parser.add_argument('--model', type=str, required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--fold', '-o', default='dev')
    
    return parser

if __name__ == '__main__':
    parser = add_arguments()
    main(parser.parse_args())