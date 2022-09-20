from pprint import pprint as pp
import pandas as pd


def pprint_v2(*arg):
    if len(arg) == 0:
        print()
    for ar in arg:
        if ar == "":
            print()
        elif type(ar) is str or type(ar) is int:
            print(ar)
        elif type(ar) is dict:
            keys = ar.keys()
            vals = ar.values()
            if (len(keys) == len(vals)) and len(
                [v for v in vals if isinstance(v, pd.DataFrame)]
            ):

                ar = [[list(a)[0], list(a)[-1], ""] for a in list(ar.items())]
                [print(item) for sublist in ar for item in sublist]
            else:
                pp(ar)
        else:
            pp(ar)
    print()