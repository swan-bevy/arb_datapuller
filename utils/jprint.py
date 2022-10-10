from pprint import pprint as pp
import pandas as pd


# =============================================================================
# PPRINT upgraded for my personal purposes
# =============================================================================
def jprint(*arg):
    if len(arg) == 0:
        print()
    for ar in arg:
        if type(ar) is str and len(ar) == 0:  # ar == ""
            print()
        elif type(ar) is str or type(ar) is int:
            print(ar)
        elif type(ar) is dict:
            keys = ar.keys()
            vals = ar.values()
            if (len(keys) == len(vals)) and len(
                [v for v in vals if isinstance(v, pd.DataFrame)]
            ):  # make sure it's just one key : one val (df)
                ar = [[list(a)[0], list(a)[-1], ""] for a in list(ar.items())]
                [print(item) for sublist in ar for item in sublist]
            else:
                pp(ar)
        elif type(ar) is pd.DataFrame:
            print(ar)
        else:
            pp(ar)
    print()
