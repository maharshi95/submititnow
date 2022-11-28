import argparse
import time
import random


def main(args):
    print('This program multiplies two numbers and saves the result.')
    value = args.num1 * args.num2
    time.sleep(random.randint(1, 10))
    print('Output is', value)
    if args.output:
        with open(args.output, 'w') as fp:
            fp.write(f'{args.num1} x {args.num2} = {value}')
    if random.randint(0, 5) % 5 == 0:
        raise ValueError('Random error')
    return value
    

def add_arguments(parser = None):
    parser = parser or argparse.ArgumentParser()
    parser.add_argument('--num1', '-n1', type=int, required=True)
    parser.add_argument('--num2', '-n2', type=int, required=True)
    parser.add_argument('--output', '-o', default=None)
    
    return parser

if __name__ == '__main__':
    parser = add_arguments()
    main(parser.parse_args())