import argparse
from typing import Optional
import torch
from tqdm import trange

def add_arguments(
    parser: Optional[argparse.ArgumentParser] = None,
) -> argparse.ArgumentParser:
    if parser is None:
        parser = argparse.ArgumentParser('Perform a matrix multiplication on a GPU')
    
    parser.add_argument("--matrix-size", type=int, default=1000)
    parser.add_argument("--n-iter", type=int, default=10)

    return parser


def main(args: argparse.Namespace):
    # Set torch seed to 
    torch.manual_seed(42)
    
    for i in trange(args.n_iter):
        M1 = torch.randn(args.matrix_size, args.matrix_size).cuda()
        M2 = torch.randn(args.matrix_size, args.matrix_size).cuda()
    
    result = M1 @ M2
    norm = torch.norm(result, p="fro")
    print(f"Norm of result: {norm}")


if __name__ == "__main__":
    parser = add_arguments()
    args = parser.parse_args()
    main(args)
