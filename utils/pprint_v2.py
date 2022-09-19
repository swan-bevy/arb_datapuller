from pprint import pprint as pp


def pprint(*arg):
    if len(arg) == 0:
        print()
    for ar in arg:
        if type(ar) is str or type(ar) is int:
            print(ar)
        else:
            pp(ar)
